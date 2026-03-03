from __future__ import annotations
import logging, os
from datetime import datetime
from typing import List
import pandas as pd

log = logging.getLogger("cranegenius.sheets")
CREDENTIALS_PATH = "credentials/google_service_account.json"
SHEET_ID_ENV = "CRANEGENIUS_SHEET_ID"
WORKSHEET_NAME = "Leads"
COLUMNS = ["date_added","jurisdiction","contractor_name","email","email_status","domain","project_address","score","generation_method"]

def export_to_sheets(warm_df, hot_df, catchall_df) -> bool:
    sheet_id = os.environ.get(SHEET_ID_ENV, "").strip()
    if not sheet_id:
        log.info("CRANEGENIUS_SHEET_ID not set — skipping Google Sheets export")
        return False
    if not os.path.exists(CREDENTIALS_PATH):
        log.warning("Google credentials not found at %s — skipping", CREDENTIALS_PATH)
        return False
    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError:
        log.warning("Run: pip install gspread google-auth")
        return False
    try:
        creds = Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"])
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(sheet_id)
        try:
            ws = sh.worksheet(WORKSHEET_NAME)
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title=WORKSHEET_NAME, rows=10000, cols=len(COLUMNS))
            ws.append_row(COLUMNS)
        rows = _build_rows(hot_df,"hot") + _build_rows(warm_df,"warm") + _build_rows(catchall_df,"catchall")
        if not rows:
            log.info("No leads to export")
            return True
        ws.append_rows(rows, value_input_option="USER_ENTERED")
        log.info("Exported %d leads to Google Sheets", len(rows))
        return True
    except Exception as exc:
        log.warning("Sheets export failed: %s", exc)
        return False

def _build_rows(df, bucket) -> List[List]:
    if df is None or df.empty:
        return []
    today = datetime.now().strftime("%Y-%m-%d")
    rows = []
    for _, r in df.iterrows():
        rows.append([today, str(r.get("jurisdiction","")), str(r.get("contractor_name_normalized",r.get("contractor_name_raw",""))), str(r.get("email_candidate",r.get("email",""))), bucket, str(r.get("contractor_domain","")), str(r.get("project_address","")), str(r.get("score","")), str(r.get("generation_method",""))])
    return rows
