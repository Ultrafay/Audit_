from fastapi import APIRouter, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse
import io
import os
import shutil
import uuid
import traceback
from pathlib import Path

import ocr_engine
from services import excel_export_service

router = APIRouter()

UPLOAD_DIR = Path("uploads")
UPLOAD_DIR.mkdir(exist_ok=True)

@router.post("/batch/extract")
async def batch_extract(files: list[UploadFile] = File(...)):
    if not files:
        raise HTTPException(status_code=400, detail="No files provided")

    print(f"[BatchExtract] Processing {len(files)} files")

    sales_orders = []
    invoices = []
    gdns = []
    
    saved_files = []

    try:
        if not ocr_engine.extractor:
            raise HTTPException(status_code=500, detail="OCR Extractor not initialized properly.")

        for file in files:
            file_id = str(uuid.uuid4())
            filename = f"{file_id}_{file.filename}"
            file_path = UPLOAD_DIR / filename
            
            with open(file_path, "wb") as buffer:
                shutil.copyfileobj(file.file, buffer)
            
            saved_files.append(file_path)

            try:
                classification = ocr_engine.extractor.classify_document(str(file_path))
                doc_type = classification.get("type", "unknown")
                confidence = classification.get("confidence", "low")

                if doc_type == "unknown" or confidence == "low":
                    print(f"[BatchExtract] Skipping file {file.filename} - type: {doc_type}, confidence: {confidence}")
                    continue

                print(f"[BatchExtract] Extracting {file.filename} as {doc_type} (confidence: {confidence})")
                
                data = ocr_engine.extractor.extract(str(file_path), doc_type)
                
                if doc_type == "sales_order":
                    sales_orders.append(data)
                elif doc_type == "sales_invoice":
                    invoices.append(data)
                elif doc_type == "gdn":
                    gdns.append(data)
                else:
                    print(f"[BatchExtract] Skip mapping unknown doc_type returned by classifier: {doc_type}")
            except Exception as item_err:
                print(f"[BatchExtract] Error processing file {file.filename}: {item_err}")
                traceback.print_exc()

        excel_bytes = excel_export_service.populate_template(
            sales_orders=sales_orders,
            invoices=invoices,
            gdns=gdns
        )

        return StreamingResponse(
            io.BytesIO(excel_bytes),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": 'attachment; filename="revenue_audit_export.xlsx"'}
        )

    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Batch extraction failed: {str(e)}")
    finally:
        for fp in saved_files:
            try:
                if fp.exists():
                    os.remove(fp)
            except Exception as e:
                print(f"[BatchExtract] Failed to delete temp file {fp}: {e}")
