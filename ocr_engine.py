import os
import time
from pathlib import Path
from dotenv import load_dotenv

from services.openai_extractor import OpenAIExtractor
from services.sheets_service import GoogleSheetsService
from utils.credentials_helper import get_credentials_path

load_dotenv()

extractor = None
sheets = None

try:
    openai_key = os.getenv("OPENAI_API_KEY")
    if openai_key:
        extractor = OpenAIExtractor(
            api_key=openai_key,
            org_id=os.getenv("OPENAI_ORG_ID"),
            project_id=os.getenv("OPENAI_PROJECT_ID")
        )
        print("OpenAI Extractor initialized.")
    
    creds_path = get_credentials_path()
    sheets = GoogleSheetsService(
        credentials_path=creds_path,
        spreadsheet_id=os.getenv("GOOGLE_SHEET_ID")
    )
    print("Sheets service initialized.")
except Exception as e:
    print(f"Warning: Failed to initialize services: {e}")
    extractor = None
    sheets = None

def process_document(
    file_path: Path,
    file_id: str,
    doc_type: str,
    sample_number: int,
) -> dict:
    if not extractor:
        raise RuntimeError("OpenAI Extractor not initialized.")
    if not sheets:
        raise RuntimeError("Google Sheets service not initialized.")

    target_tab = "O A C"
    target_row = 13 + sample_number - 1
    start_time = time.perf_counter()

    try:
        model = extractor.extract(str(file_path), doc_type)
        extracted_dict = model.model_dump()
        
        duration_ms = int((time.perf_counter() - start_time) * 1000)
        
        write_success = sheets.write_to_sample_row(
            tab_name=target_tab,
            sample_number=sample_number,
            doc_type=doc_type,
            extracted_data=extracted_dict
        )
        
        sheets.append_to_extraction_log(
            file_id=file_id,
            source_filename=file_path.name,
            doc_type=doc_type,
            target_tab=target_tab,
            target_row=target_row,
            status="success",
            model_used=extractor.model,
            duration_ms=duration_ms
        )
        
        return {
           "file_id": file_id,
           "doc_type": doc_type,
           "sample_number": sample_number,
           "target_row": target_row,
           "extracted": extracted_dict,
           "sheet_write_success": write_success,
        }
        
    except Exception as e:
        duration_ms = int((time.perf_counter() - start_time) * 1000)
        error_msg = str(e)
        if sheets:
            sheets.append_to_extraction_log(
                file_id=file_id,
                source_filename=file_path.name,
                doc_type=doc_type,
                target_tab=target_tab,
                target_row=target_row,
                status="failed",
                model_used=extractor.model,
                duration_ms=duration_ms,
                error_message=error_msg
            )
        raise
