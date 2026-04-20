"""
Background Drive processor for AUDIT_GS.
Polls DRIVE_FOLDER_INBOX for new audit documents, classifies them via OpenAI,
extracts structured data, writes to the Google Sheets working paper,
and routes files to the appropriate processed/failed folder.
"""
import asyncio
import os
import re
import uuid
import io
import tempfile
import traceback
from datetime import datetime
from pathlib import Path
from typing import Optional, Set

from dotenv import load_dotenv
load_dotenv()

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

import ocr_engine

FILENAME_PATTERN = re.compile(r'^sample_(\d{1,2})_.+\.(pdf|jpg|jpeg|png)$', re.IGNORECASE)

SUPPORTED_MIME_TYPES = [
    'application/pdf',
    'image/jpeg',
    'image/png',
]

# Map classified doc_type → env var for the processed folder
PROCESSED_FOLDER_ENV = {
    "sales_order":   "DRIVE_FOLDER_PROCESSED_SALES_ORDERS",
    "sales_invoice": "DRIVE_FOLDER_PROCESSED_SALES_INVOICES",
    "gdn":           "DRIVE_FOLDER_PROCESSED_GDNS",
}


class DriveProcessor:
    def __init__(self):
        self.poll_interval = int(os.getenv("DRIVE_POLL_INTERVAL", "30"))
        self.is_running = False
        self._task: Optional[asyncio.Task] = None
        self._processed_ids: Set[str] = set()
        self._stats = {
            "started_at": None,
            "last_poll": None,
            "files_processed": 0,
            "files_failed": 0,
        }

        # Folder IDs from .env
        self.inbox_folder_id = os.getenv("DRIVE_FOLDER_INBOX", "")
        if not self.inbox_folder_id:
            raise ValueError("DRIVE_FOLDER_INBOX is required")

        self.folder_failed_unclassified = os.getenv("DRIVE_FOLDER_FAILED_UNCLASSIFIED", "")
        self.folder_failed_extraction = os.getenv("DRIVE_FOLDER_FAILED_EXTRACTION", "")
        self.folder_failed_sheet_write = os.getenv("DRIVE_FOLDER_FAILED_SHEET_WRITE", "")

        self.processed_folders = {}
        for doc_type, env_key in PROCESSED_FOLDER_ENV.items():
            folder_id = os.getenv(env_key, "")
            if not folder_id:
                raise ValueError(f"{env_key} is required")
            self.processed_folders[doc_type] = folder_id

        # Build Drive API service
        from utils.credentials_helper import get_credentials_path
        creds_path = get_credentials_path()
        creds = service_account.Credentials.from_service_account_file(
            creds_path,
            scopes=['https://www.googleapis.com/auth/drive']
        )
        self.drive_service = build('drive', 'v3', credentials=creds)

        print(f"[DriveProcessor] Initialized. Inbox: {self.inbox_folder_id}, poll every {self.poll_interval}s")

    # ── Lifecycle ───────────────────────────────────────────────

    async def start(self):
        """Start the background polling loop."""
        if self.is_running:
            return
        self.is_running = True
        self._stats["started_at"] = datetime.now().isoformat()
        self._task = asyncio.create_task(self._poll_loop())
        print(f"[DriveProcessor] Started polling every {self.poll_interval}s")

    async def stop(self):
        """Stop the background polling loop."""
        self.is_running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        print("[DriveProcessor] Stopped")

    def get_status(self) -> dict:
        return {
            "is_running": self.is_running,
            "inbox_folder_id": self.inbox_folder_id,
            "poll_interval_seconds": self.poll_interval,
            "tracked_file_count": len(self._processed_ids),
            **self._stats
        }

    # ── Core Loop ───────────────────────────────────────────────

    async def _poll_loop(self):
        """Main polling loop. Runs in background."""
        while self.is_running:
            try:
                await self._poll_once()
            except Exception as e:
                print(f"[DriveProcessor] Poll error: {e}")
                traceback.print_exc()

            await asyncio.sleep(self.poll_interval)

    async def _poll_once(self):
        """Single poll iteration: list inbox files → process new ones."""
        self._stats["last_poll"] = datetime.now().isoformat()

        loop = asyncio.get_event_loop()
        files = await loop.run_in_executor(None, self._list_inbox_files)

        if not files:
            return

        new_files = [f for f in files if f['id'] not in self._processed_ids]
        if not new_files:
            return

        print(f"[DriveProcessor] Found {len(new_files)} new file(s)")

        for file_info in new_files:
            await loop.run_in_executor(None, self._process_file, file_info)

    # ── File Processing ─────────────────────────────────────────

    def _process_file(self, file_info: dict):
        """Full processing flow: parse filename → classify → extract → route."""
        drive_file_id = file_info['id']   # Drive's own file identifier — used for dedup, download, and move
        file_name = file_info['name']
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        print(f"[DriveProcessor] [{timestamp}] Processing: {file_name}")

        # Always mark as seen to avoid re-processing
        self._processed_ids.add(drive_file_id)

        # ── Step 1: Parse sample number from filename ──
        sample_number = self._parse_sample_number(file_name)
        if sample_number is None:
            print(f"[DriveProcessor]   ✗ Invalid filename: {file_name}")
            self._log_failure(
                file_id=str(uuid.uuid4()),
                filename=file_name,
                doc_type="unknown",
                error_message="invalid_filename"
            )
            self._move_file(drive_file_id, self.folder_failed_unclassified)
            self._stats["files_failed"] += 1
            return

        # ── Step 2: Download to temp file ──
        suffix = Path(file_name).suffix or ".tmp"
        tmp_fd, tmp_path = tempfile.mkstemp(suffix=suffix, prefix="audit_")
        os.close(tmp_fd)

        try:
            self._download_file(drive_file_id, tmp_path)
            print(f"[DriveProcessor]   Downloaded to temp")

            # ── Step 3: Classify document ──
            try:
                classification = ocr_engine.extractor.classify_document(tmp_path)
            except Exception as e:
                print(f"[DriveProcessor]   ✗ Classification failed: {e}")
                traceback.print_exc()
                self._log_failure(
                    file_id=str(uuid.uuid4()),
                    filename=file_name,
                    doc_type="unknown",
                    error_message=f"classification_error: {e}"
                )
                self._move_file(drive_file_id, self.folder_failed_unclassified)
                self._stats["files_failed"] += 1
                return

            doc_type = classification.get("type", "unknown")
            confidence = classification.get("confidence", "low")

            if doc_type == "unknown" or confidence == "low":
                print(f"[DriveProcessor]   ✗ Unclassified (type={doc_type}, confidence={confidence})")
                self._log_failure(
                    file_id=str(uuid.uuid4()),
                    filename=file_name,
                    doc_type=doc_type,
                    error_message=f"unclassified: type={doc_type}, confidence={confidence}"
                )
                self._move_file(drive_file_id, self.folder_failed_unclassified)
                self._stats["files_failed"] += 1
                return

            print(f"[DriveProcessor]   Classified as {doc_type} (confidence={confidence})")

            # ── Step 4: Extract + write to working paper ──
            # Generate a fresh UUID per extraction attempt, matching the API endpoint's pattern.
            extraction_file_id = str(uuid.uuid4())
            try:
                result = ocr_engine.process_document(
                    file_path=Path(tmp_path),
                    file_id=extraction_file_id,
                    doc_type=doc_type,
                    sample_number=sample_number,
                )
            except Exception as e:
                # Extraction crashed — process_document already logs to Extraction Log
                print(f"[DriveProcessor]   ✗ Extraction failed: {e}")
                traceback.print_exc()
                self._move_file(drive_file_id, self.folder_failed_extraction)
                self._stats["files_failed"] += 1
                return

            # ── Step 5: Check sheet write result and route file ──
            if not result.get("sheet_write_success", False):
                print(f"[DriveProcessor]   ✗ Sheet write failed")
                self._move_file(drive_file_id, self.folder_failed_sheet_write)
                self._stats["files_failed"] += 1
                return

            # Success — move to the correct processed folder
            target_folder = self.processed_folders.get(doc_type)
            if target_folder:
                self._move_file(drive_file_id, target_folder)
                print(f"[DriveProcessor]   ✓ Done → processed/{doc_type}")
            else:
                print(f"[DriveProcessor]   ⚠ No processed folder for {doc_type}, leaving in inbox")

            self._stats["files_processed"] += 1

        finally:
            # Clean up temp file
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

    # ── Helpers ──────────────────────────────────────────────────

    def _parse_sample_number(self, filename: str) -> Optional[int]:
        """
        Parse sample number from filename matching sample_{N}_anything.ext
        Returns N (1-20) or None if filename doesn't match.
        """
        match = FILENAME_PATTERN.match(filename)
        if not match:
            return None
        n = int(match.group(1))
        if not (1 <= n <= 20):
            return None
        return n

    def _list_inbox_files(self) -> list:
        """List supported files in the inbox folder."""
        mime_filter = " or ".join(
            f"mimeType='{m}'" for m in SUPPORTED_MIME_TYPES
        )
        query = (
            f"'{self.inbox_folder_id}' in parents "
            f"and ({mime_filter}) "
            f"and trashed=false"
        )
        results = self.drive_service.files().list(
            q=query,
            fields="files(id, name, mimeType, createdTime)",
            orderBy="createdTime",
            pageSize=50,
        ).execute()
        return results.get('files', [])

    def _download_file(self, file_id: str, dest_path: str):
        """Download a Drive file to a local path."""
        request = self.drive_service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)

        done = False
        while not done:
            _, done = downloader.next_chunk()

        with open(dest_path, 'wb') as f:
            f.write(fh.getvalue())

    def _move_file(self, file_id: str, target_folder_id: str):
        """Move a file from inbox to the target folder via Drive API."""
        if not target_folder_id:
            print(f"[DriveProcessor]   ⚠ No target folder ID, cannot move file {file_id}")
            return
        try:
            self.drive_service.files().update(
                fileId=file_id,
                addParents=target_folder_id,
                removeParents=self.inbox_folder_id,
                fields='id, parents',
            ).execute()
        except Exception as e:
            print(f"[DriveProcessor]   ⚠ Failed to move file {file_id}: {e}")
            traceback.print_exc()

    def _log_failure(self, file_id: str, filename: str, doc_type: str, error_message: str):
        """Log a pre-extraction failure to the Extraction Log sheet."""
        if not ocr_engine.sheets:
            print(f"[DriveProcessor]   ⚠ Sheets not initialized, cannot log failure")
            return
        try:
            ocr_engine.sheets.append_to_extraction_log(
                file_id=file_id,
                source_filename=filename,
                doc_type=doc_type,
                target_tab="O A C",
                target_row=0,
                status="failed",
                model_used=ocr_engine.extractor.model if ocr_engine.extractor else "gpt-4o",
                duration_ms=0,
                error_message=error_message,
            )
        except Exception as e:
            print(f"[DriveProcessor]   ⚠ Failed to log to Extraction Log: {e}")
