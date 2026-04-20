from fastapi import FastAPI, UploadFile, File, HTTPException, Request, Form, Path as FastPath
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
import uvicorn
import shutil
import os
import uuid
from pathlib import Path
import ocr_engine
from fastapi.middleware.cors import CORSMiddleware

# ── Drive Processor (lazy init) ──────────────────────────────

drive_processor = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start Drive watcher on startup, stop on shutdown."""
    global drive_processor
    
    folder_id = os.getenv("DRIVE_FOLDER_INBOX")
    if folder_id:
        try:
            from workers.drive_processor import DriveProcessor
            drive_processor = DriveProcessor()
            await drive_processor.start()
        except Exception as e:
            print(f"[App] Drive watcher failed to start: {e}")
            import traceback
            traceback.print_exc()
            drive_processor = None
    else:
        print("[App] DRIVE_FOLDER_INBOX not set — Drive watcher disabled")
    
    yield  # App is running
    
    # Shutdown
    if drive_processor:
        await drive_processor.stop()


app = FastAPI(lifespan=lifespan)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "*"  # To be restricted in real production to Vercel/Railway frontend domains
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Create directories if they don't exist
UPLOAD_DIR = Path("uploads")
UPLOAD_DIR.mkdir(exist_ok=True)

@app.post("/api/extract/{doc_type}")
async def extract_document(
    doc_type: str = FastPath(..., description="sales_order, sales_invoice, or gdn"),
    file: UploadFile = File(...),
    sample_number: int = Form(..., ge=1, le=20),
):
    if doc_type not in ("sales_order", "sales_invoice", "gdn"):
        raise HTTPException(400, f"Invalid doc_type: {doc_type}")

    file_id = str(uuid.uuid4())
    filename = f"{file_id}_{file.filename}"
    file_path = UPLOAD_DIR / filename
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    try:
        result = ocr_engine.process_document(file_path, file_id, doc_type, sample_number)
        return JSONResponse(content=result)
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(500, str(e))

@app.get("/api/drive-watcher/status")
async def drive_watcher_status():
    """Get Drive watcher status"""
    if not drive_processor:
        return JSONResponse(content={
            "is_running": False,
            "message": "Drive watcher not configured. Set DRIVE_FOLDER_INBOX in .env"
        })
    return JSONResponse(content=drive_processor.get_status())

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)