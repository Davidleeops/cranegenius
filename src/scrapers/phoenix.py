"""
Phoenix Open Data — Development Services Permits
CSV endpoint: Phoenix Open Data permit dataset

Field mapping (as of 2025):
  PermitNumber        → permit_or_record_id
  PermitType          → source_type hint
  StatusCurrent       → record_status
  IssuedDate          → record_date
  ProjectDescription  → description_raw
  ContractorName      → contractor_name_raw
  SiteAddress         → project_address
  SiteCity            → project_city
  SiteState           → project_state

If Phoenix changes their CSV format, update FIELD_MAP below.
Check field names by downloading the CSV manually and looking at row 1.
"""
from __future__ import annotations

import io
import logging
from typing import Any, Dict, List

import pandas as pd
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from ..utils import normalize_text, utc_now_iso

log = logging.getLogger("cranegenius.phoenix")

# Map Phoenix CSV column names → our canonical field names
# If the CSV changes, only this dict needs updating
FIELD_MAP = {
    "PermitNumber": "permit_or_record_id",
    "StatusCurrent": "record_status",
    "IssuedDate": "record_date",
    "ProjectDescription": "description_raw",
    "ContractorName": "contractor_name_raw",
    "SiteAddress": "project_address",
    "SiteCity": "project_city",
    "SiteState": "project_state",
}

# Fallback aliases if Phoenix renames columns
FIELD_ALIASES = {
    "permit_or_record_id": ["PermitNumber", "PermitNum", "Permit_Number", "permit_number"],
    "record_status": ["StatusCurrent", "Status", "PermitStatus", "permit_status"],
    "record_date": ["IssuedDate", "IssueDate", "Issued_Date", "issued_date", "AppliedDate"],
    "description_raw": ["ProjectDescription", "Description", "WorkDescription", "work_description"],
    "contractor_name_raw": ["ContractorName", "Contractor", "contractor_name", "GCName"],
    "project_address": ["SiteAddress", "Address", "ProjectAddress", "project_address"],
    "project_city": ["SiteCity", "City", "project_city"],
    "project_state": ["SiteState", "State", "project_state"],
}


class PhoenixScraper:
    """Scraper for Phoenix Open Data development permits CSV."""

    def __init__(self, source_config: Dict[str, Any]):
        self.source = source_config
        self.url = source_config["url"]
        self.source_id = source_config["id"]
        self.jurisdiction = source_config["jurisdiction"]

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def fetch(self) -> pd.DataFrame:
        log.info("Phoenix: fetching CSV from %s", self.url)
        headers = {
            "User-Agent": "CraneGeniusLeadBot/1.0",
            "Accept": "text/csv,application/csv,*/*",
        }
        r = requests.get(self.url, timeout=60, headers=headers)
        r.raise_for_status()
        df = pd.read_csv(io.StringIO(r.text), low_memory=False)
        log.info("Phoenix: fetched %d raw rows, %d columns", len(df), len(df.columns))
        return df

    def _resolve_column(self, df: pd.DataFrame, canonical: str) -> str:
        """Find actual column name in df for a canonical field, using aliases."""
        # Direct match first
        if canonical in FIELD_MAP:
            candidate = FIELD_MAP[canonical]  # shouldn't happen this direction but safe
        for alias in FIELD_ALIASES.get(canonical, []):
            if alias in df.columns:
                return alias
        return ""

    def parse(self, raw_df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Map Phoenix CSV fields → canonical schema rows."""
        cols = list(raw_df.columns)
        log.info("Phoenix columns found: %s", cols)

        # Build column resolver
        col_map: Dict[str, str] = {}
        for canonical, aliases in FIELD_ALIASES.items():
            for alias in aliases:
                if alias in cols:
                    col_map[canonical] = alias
                    break

        missing = [f for f in FIELD_ALIASES if f not in col_map]
        if missing:
            log.warning("Phoenix: could not find columns for: %s", missing)

        rows: List[Dict[str, Any]] = []
        captured_at = utc_now_iso()

        for _, row in raw_df.iterrows():
            def get(canonical: str) -> str:
                col = col_map.get(canonical, "")
                if not col:
                    return ""
                return normalize_text(row.get(col))

            permit_id = get("permit_or_record_id")
            description = get("description_raw")
            contractor = get("contractor_name_raw")

            # Skip rows with no useful data
            if not description and not contractor:
                continue

            rows.append({
                "source_id": self.source_id,
                "source_type": "permit",
                "jurisdiction": self.jurisdiction,
                "source_url": self.url,
                "source_capture_utc": captured_at,
                "permit_or_record_id": permit_id,
                "record_status": get("record_status"),
                "record_date": get("record_date"),
                "project_address": get("project_address"),
                "project_city": get("project_city") or "Phoenix",
                "project_state": get("project_state") or "AZ",
                "contractor_name_raw": contractor,
                "description_raw": description,
            })

        log.info("Phoenix: parsed %d usable rows from %d total", len(rows), len(raw_df))
        return rows

    def run(self) -> List[Dict[str, Any]]:
        """Full fetch + parse. Returns list of canonical row dicts."""
        raw_df = self.fetch()
        return self.parse(raw_df)
