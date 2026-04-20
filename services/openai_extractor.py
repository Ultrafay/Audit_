from openai import OpenAI
import json
import base64
from pathlib import Path
from typing import Optional, List, Type
from pydantic import BaseModel, Field
import os
import tempfile

# --- Data Models ---

class LineItem(BaseModel):
    description: Optional[str] = None
    quantity: Optional[float] = None
    rate: Optional[float] = None
    amount: Optional[float] = None

class GDNLineItem(BaseModel):
    description: Optional[str] = None
    quantity_delivered: Optional[float] = None

class SalesOrderData(BaseModel):
    so_number: Optional[str] = None
    so_date: Optional[str] = None              # YYYY-MM-DD
    customer_name: Optional[str] = None
    currency: Optional[str] = None             # PKR, USD, AED, etc.
    line_items: List[LineItem] = Field(default_factory=list)
    total_quantity: Optional[float] = None
    total_amount: Optional[float] = None
    notes: Optional[str] = None                # anything the auditor should know

class SalesInvoiceData(BaseModel):
    invoice_number: Optional[str] = None
    invoice_date: Optional[str] = None         # YYYY-MM-DD
    customer_name: Optional[str] = None
    so_reference: Optional[str] = None         # SO# printed on the invoice, if any
    currency: Optional[str] = None
    line_items: List[LineItem] = Field(default_factory=list)
    total_quantity: Optional[float] = None
    subtotal: Optional[float] = None
    tax_amount: Optional[float] = None
    total_amount: Optional[float] = None
    notes: Optional[str] = None

class GDNData(BaseModel):
    gdn_reference: Optional[str] = None
    delivered_date: Optional[str] = None       # YYYY-MM-DD
    customer_name: Optional[str] = None
    so_reference: Optional[str] = None         # SO# on the GDN, if any
    invoice_reference: Optional[str] = None    # Invoice# on the GDN, if any
    line_items: List[GDNLineItem] = Field(default_factory=list)
    total_quantity_delivered: Optional[float] = None
    notes: Optional[str] = None

class DocumentClassification(BaseModel):
    type: str       # "sales_order" | "sales_invoice" | "gdn" | "unknown"
    confidence: str  # "high" | "medium" | "low"


# --- Prompts ---

COMMON_PREAMBLE = """You are an OCR extraction assistant for a professional audit firm. You are extracting data from a supporting document as part of a revenue audit under ISA (International Standards on Auditing). Your output will be reviewed and signed off by a qualified auditor.

Rules you must follow:
1. NEVER invent, guess, or infer values. If a field is not clearly present on the document, return null for that field. It is always better to return null than a wrong value — the auditor can fill in missing data manually, but they cannot easily detect a fabricated one.
2. Return dates in YYYY-MM-DD format. If the document shows a different format (e.g. 12/01/2025 or 1-Dec-2025), convert it. If the date is ambiguous between DD/MM and MM/DD, return null and note the ambiguity in the `notes` field.
3. Return monetary amounts as numbers (not strings), without currency symbols or thousands separators. Put the currency code in the `currency` field separately. Recognized currencies include PKR, USD, AED, EUR, GBP — if the document uses a symbol like Rs or $, infer the ISO code.
4. Extract every line item visible on the document. Do not summarize or collapse lines. If a line is unclear, include it with null values for the unclear fields.
5. If the document is rotated, skewed, or partially unreadable, extract what you can and mention the quality issue in `notes`.
6. If the document appears to be a different type than requested (e.g. you were asked for a Sales Invoice but the document is clearly a Delivery Note), set all fields to null and put "DOCUMENT_TYPE_MISMATCH: expected X, appears to be Y" in `notes`.
"""

SALES_ORDER_PROMPT = COMMON_PREAMBLE + """
You are extracting data from a SALES ORDER — a commercial document issued by a seller confirming a customer's purchase request, before goods are delivered or invoiced.

Extract the following fields:
- so_number: the sales order number (may also be labelled "Order No.", "SO#", "PO No.", or similar)
- so_date: the date the sales order was issued
- customer_name: the buyer / customer (not the seller)
- currency: currency of the amounts
- line_items: every line item on the order, with description, quantity ordered, unit rate, and line amount
- total_quantity: sum of all line quantities if shown, otherwise null
- total_amount: the grand total of the order

Do not confuse a Sales Order with an Invoice or Delivery Note. A Sales Order typically says "Sales Order", "Purchase Order", or "Order Confirmation" at the top, and usually has no tax breakdown or payment terms.
"""

SALES_INVOICE_PROMPT = COMMON_PREAMBLE + """
You are extracting data from a SALES INVOICE — the commercial document issued by a seller to a customer requesting payment for goods or services delivered.

Extract the following fields:
- invoice_number: the invoice number (may be labelled "Invoice No.", "Inv#", "Bill No.", or similar)
- invoice_date: the date on the invoice
- customer_name: the customer / buyer being billed (NOT the seller issuing the invoice)
- so_reference: if the invoice references a sales order or purchase order number, capture it here — this is critical for audit matching. If not present, return null.
- currency: currency code
- line_items: every line item with description, quantity, unit rate, and line amount
- total_quantity: sum of line quantities
- subtotal: pre-tax amount if shown separately
- tax_amount: GST / VAT / sales tax amount if shown
- total_amount: the final billed amount

Make sure you identify the CUSTOMER (bill-to party), not the seller. On most invoices the seller's details are at the top in a letterhead and the customer is in a "Bill To" or "Invoice To" block.
"""

GDN_PROMPT = COMMON_PREAMBLE + """
You are extracting data from a GOODS DELIVERY NOTE (GDN) — also called a Delivery Note, Delivery Challan, Dispatch Note, or Goods Despatch Note. This is a document that accompanies goods when they are delivered from a seller to a customer, and evidences that delivery actually took place. GDNs usually have no prices or amounts.

Extract the following fields:
- gdn_reference: the GDN / delivery note number
- delivered_date: the date of delivery
- customer_name: the party the goods were delivered to
- so_reference: if the GDN references a sales order number, capture it — critical for audit matching
- invoice_reference: if the GDN references an invoice number, capture it
- line_items: every line with description and quantity delivered (GDNs typically do NOT have rates or amounts — leave those off)
- total_quantity_delivered: sum of all delivered quantities

Key distinction: a GDN evidences physical delivery. If the document shows prices and totals but no delivery acknowledgement, it is probably an invoice, not a GDN — flag it as DOCUMENT_TYPE_MISMATCH.
"""

CLASSIFY_PROMPT = """You are a document classifier for an audit pipeline. Your ONLY job is to identify the document type. You are NOT extracting data.

Respond with JSON in this exact shape:
{"type": "sales_order" | "sales_invoice" | "gdn" | "unknown", "confidence": "high" | "medium" | "low"}

Definitions:
- sales_order: a document confirming a customer order before delivery (titles like "Sales Order", "Purchase Order", "Order Confirmation"). Usually has prices but no delivery acknowledgement and no payment terms.
- sales_invoice: a bill issued to a customer for goods/services (titles like "Invoice", "Tax Invoice", "Bill"). Has prices, taxes, and payment terms.
- gdn: a Goods Delivery Note / Delivery Note / Dispatch Note / Delivery Challan. Evidences physical delivery. Usually has quantities but NO prices.

Return "unknown" if you cannot tell with confidence, or if the document is something else entirely (purchase invoice, payment receipt, contract, etc.). Better unknown than wrong.

DO NOT extract any other fields. DO NOT return company names, addresses, line items, totals, or any other data. ONLY return the two fields: type and confidence."""


# --- Extractor Class ---

class OpenAIExtractor:
    def __init__(self, api_key: str, org_id: str = None, project_id: str = None):
        if not api_key:
            raise ValueError("OpenAI API key is required")
        
        self.client = OpenAI(
            api_key=api_key,
            organization=org_id,
            project=project_id
        )
        self.model = "gpt-4o"

    def _encode_image_to_base64(self, image_path: str) -> str:
        """Read an image file and return its base64 encoding."""
        with open(image_path, "rb") as f:
            return base64.b64encode(f.read()).decode("utf-8")

    def _get_mime_type(self, file_path: str) -> str:
        """Determine MIME type from file extension."""
        ext = Path(file_path).suffix.lower()
        mime_map = {
            ".jpg": "image/jpeg",
            ".jpeg": "image/jpeg",
            ".png": "image/png",
            ".gif": "image/gif",
            ".webp": "image/webp",
            ".bmp": "image/bmp",
        }
        return mime_map.get(ext, "image/jpeg")

    def _call_openai(
        self,
        image_path: str,
        system_prompt: str,
        schema_class: Type[BaseModel],
        user_message: str = "Extract all relevant data from this structured document into JSON.",
    ) -> BaseModel:
        b64_image = self._encode_image_to_base64(image_path)
        mime_type = self._get_mime_type(image_path)
        
        json_schema_str = json.dumps(schema_class.model_json_schema(), indent=2)
        full_system_prompt = f"{system_prompt}\n\nPlease output valid JSON that conforms exactly to the following JSON Schema. Do NOT embed it in markdown fences; return ONLY the raw JSON object.\n{json_schema_str}"

        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": full_system_prompt},
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": user_message},
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:{mime_type};base64,{b64_image}",
                                "detail": "high"
                            }
                        }
                    ]
                }
            ],
            max_tokens=4096,
            temperature=0.0,
            response_format={"type": "json_object"}
        )
        
        response_text = response.choices[0].message.content
        try:
            # Clean up markdown formatting if present
            clean_text = response_text
            if "```json" in clean_text:
                clean_text = clean_text.split("```json")[1].split("```")[0]
            elif "```" in clean_text:
                clean_text = clean_text.split("```")[1].split("```")[0]
            clean_text = clean_text.strip()
            
            data = json.loads(clean_text)
            return schema_class(**data)
        except Exception as e:
            print(f"Error parsing OpenAI response: {e}")
            print(f"Raw response: {response_text}")
            raise ValueError(f"Failed to parse JSON from OpenAI response: {e}")

    def _extract_from_image(
        self,
        image_path: str,
        system_prompt: str,
        schema_class: Type[BaseModel],
        user_message: str = "Extract all relevant data from this structured document into JSON.",
    ) -> BaseModel:
        return self._call_openai(image_path, system_prompt, schema_class, user_message=user_message)

    def _extract_from_pdf(
        self,
        pdf_path: str,
        system_prompt: str,
        schema_class: Type[BaseModel],
        user_message: str = "Extract all relevant data from this structured document into JSON.",
    ) -> BaseModel:
        from pdf2image import convert_from_path
        
        # Convert first page of PDF to image
        images = convert_from_path(pdf_path, first_page=1, last_page=1, dpi=200)
        
        if not images:
            raise ValueError("Could not convert PDF to image")
        
        # Save to temp file
        with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as tmp:
            images[0].save(tmp.name, "JPEG", quality=95)
            tmp_path = tmp.name
        
        try:
            result = self._call_openai(tmp_path, system_prompt, schema_class, user_message=user_message)
        finally:
            os.unlink(tmp_path)
            
        return result

    def extract(self, file_path: str, doc_type: str) -> BaseModel:
        """
        Extract structured data from an audit document.

        Args:
            file_path: path to PDF or image
            doc_type:  one of 'sales_order', 'sales_invoice', 'gdn'

        Returns:
            Pydantic model instance (SalesOrderData | SalesInvoiceData | GDNData)

        Raises:
            ValueError if doc_type is invalid
            Exception on API / parsing failure (let it bubble up)
        """
        valid_types = {
            'sales_order': (SalesOrderData, SALES_ORDER_PROMPT),
            'sales_invoice': (SalesInvoiceData, SALES_INVOICE_PROMPT),
            'gdn': (GDNData, GDN_PROMPT)
        }

        if doc_type not in valid_types:
            raise ValueError(f"Invalid doc_type '{doc_type}'. Must be one of {list(valid_types.keys())}")

        schema_class, system_prompt = valid_types[doc_type]
        filename = Path(file_path).name

        print(f"[OpenAIExtractor] Extracting {doc_type} from {filename}")

        if str(file_path).lower().endswith(".pdf"):
            return self._extract_from_pdf(file_path, system_prompt, schema_class)
        else:
            return self._extract_from_image(file_path, system_prompt, schema_class)

    def classify_document(self, file_path: str) -> dict:
        """
        Classify a document as sales_order, sales_invoice, gdn, or unknown.
        This is a lightweight, separate OpenAI call — not the full extraction.

        Args:
            file_path: path to PDF or image

        Returns:
            dict with keys "type" and "confidence"
            e.g. {"type": "sales_invoice", "confidence": "high"}
        """
        filename = Path(file_path).name
        print(f"[OpenAIExtractor] Classifying {filename}")

        classify_user_message = (
            "Classify this document. You are NOT extracting any data — only identifying "
            "the document type. Respond ONLY with the JSON schema provided "
            '{"type": "sales_order" | "sales_invoice" | "gdn" | "unknown", '
            '"confidence": "high" | "medium" | "low"}. Do not include any other fields.'
        )

        if str(file_path).lower().endswith(".pdf"):
            result = self._extract_from_pdf(
                file_path, CLASSIFY_PROMPT, DocumentClassification,
                user_message=classify_user_message,
            )
        else:
            result = self._extract_from_image(
                file_path, CLASSIFY_PROMPT, DocumentClassification,
                user_message=classify_user_message,
            )

        classification = result.model_dump()
        print(f"[OpenAIExtractor] Classification: {classification}")
        return classification

