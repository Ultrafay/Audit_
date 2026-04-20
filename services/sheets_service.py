from google.oauth2 import service_account
from googleapiclient.discovery import build
from datetime import datetime
import os

OAC_FIRST_ROW = 13
OAC_LAST_ROW = 32

COLUMN_MAP = {
    "O A C": {
        "sales_order": {
            "so_number":      "H",
            "customer_name":  "I",
            "total_quantity": "J",
            "rate":           "K",
            "total_amount":   "L",
        },
        "sales_invoice": {
            "customer_name":         "P",
            "invoice_number":        "Q",
            "invoice_date":          "R",
            "total_quantity":        "S",
            "rate":                  "T",
            "total_amount":          "V",
        },
        "gdn": {
            "customer_name":           "AA",
            "delivered_date":          "AB",
            "total_quantity_delivered":"AC",
            "gdn_reference":           "AD",
        },
    },
}

class GoogleSheetsService:
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

    def __init__(self, credentials_path: str, spreadsheet_id: str):
        if not os.path.exists(credentials_path):
             raise FileNotFoundError(f"Credentials file not found at {credentials_path}")
             
        self.spreadsheet_id = spreadsheet_id
        self.creds = service_account.Credentials.from_service_account_file(
            credentials_path, scopes=self.SCOPES
        )
        self.service = build('sheets', 'v4', credentials=self.creds)
        self.sheet = self.service.spreadsheets()

    def write_to_sample_row(
        self,
        tab_name: str,
        sample_number: int,
        doc_type: str,
        extracted_data: dict,
    ) -> bool:
        if not (1 <= sample_number <= 20):
            raise ValueError(f"sample_number must be between 1 and 20, got {sample_number}")
            
        if tab_name not in COLUMN_MAP:
            raise ValueError(f"Unsupported tab_name: {tab_name}")
            
        if doc_type not in COLUMN_MAP[tab_name]:
            raise ValueError(f"Unsupported doc_type '{doc_type}' for tab '{tab_name}'")
            
        target_row = OAC_FIRST_ROW + (sample_number - 1)
        col_map = COLUMN_MAP[tab_name][doc_type]
        
        data_to_update = []
        
        for field, col_letter in col_map.items():
            value = None
            if field == "rate":
                line_items = extracted_data.get("line_items", [])
                if line_items and isinstance(line_items, list) and len(line_items) > 0:
                    first_item = line_items[0]
                    value = first_item.get("rate") if isinstance(first_item, dict) else getattr(first_item, "rate", None)
            else:
                value = extracted_data.get(field)
                
            if value is not None:
                cell_ref = f"'{tab_name}'!{col_letter}{target_row}"
                data_to_update.append({
                    "range": cell_ref,
                    "values": [[value]]
                })
                
        if not data_to_update:
            return True 
            
        try:
            body = {
                "valueInputOption": "USER_ENTERED",
                "data": data_to_update
            }
            self.sheet.values().batchUpdate(
                spreadsheetId=self.spreadsheet_id,
                body=body
            ).execute()
            return True
        except Exception as e:
            print(f"[SheetsService] Error writing to sample row: {e}")
            return False

    def append_to_extraction_log(
        self,
        file_id: str,
        source_filename: str,
        doc_type: str,
        target_tab: str,
        target_row: int,
        status: str,          
        model_used: str,
        duration_ms: int,
        tokens_used: int = 0,
        error_message: str = "",
    ) -> bool:
        try:
            timestamp = datetime.now().isoformat()
            row_data = [
                timestamp,
                file_id,
                source_filename,
                doc_type,
                target_tab,
                target_row,
                status,
                model_used,
                duration_ms,
                tokens_used,
                error_message
            ]
            
            body = {
                'values': [row_data]
            }
            
            self.sheet.values().append(
                spreadsheetId=self.spreadsheet_id,
                range="'Extraction Log'!A:A",
                valueInputOption="USER_ENTERED",
                body=body
            ).execute()
            return True
        except Exception as e:
            print(f"[SheetsService] Error appending to Extraction Log: {e}")
            return False
