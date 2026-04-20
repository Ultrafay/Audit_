# ATH Document Pipeline
Powered by Solvevia

An automated, backend pipeline for document processing, primarily tailored for UAE-based requirements.

## Core Features
1. **Google Drive Integration**: Automatically polls folders for incoming documents (e.g. Sales Invoices, Sales Orders, Goods Dispatch Notes).
2. **AI Extraction**: Analyzes documents via modern large language models, structured to handle multi-line complex details accurately.
3. **Google Sheets Sync**: Real-time push of extracted data into a unified tracker sheet.
4. **Data Deduplication**: Validates and checks previous records against supplier and reference numbers.

## Backend Architecture
- **FastAPI Core**: A fast, asynchronous backend exposed for integration.
- **Background Worker**: `drive_processor.py` orchestrates asynchronous polling and data push routines.
- **Modular Pipeline**: Controlled via `ocr_engine.py` using abstract `extactor` instances.

## Setup Instructions

1. **Environment Setup**
Ensure all environment variables are correctly placed in `.env` (Google Service Account keys, Sheet IDs, Drive Folder IDs).

2. **Dependencies**
Install standard Python dependencies from `requirements.txt`:
```bash
pip install -r requirements.txt
```

3. **Running the App**
To start the FastAPI application:
```bash
python app.py
```

The system will start listening and the `DriveProcessor` will immediately begin polling the designated Google Drive inbox folder if configured.

## Configuration (.env)
Required configurations:
- `GOOGLE_DRIVE_FOLDER_ID`
- `GOOGLE_SHEET_ID`
- `OPENAI_API_KEY`
- Google Credentials file path

*(Note: Ensure sensitive data is never committed to source control.)*
