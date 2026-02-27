import io
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any
from src.utils import utc_now_iso, normalize_text

log = logging.getLogger("cranegenius.scrapers.dallas")

class DallasScraper:
    source_id = "dallas_permits"
    jurisdiction = "Dallas, TX"

    def fetch(self) -> pd.DataFrame:
        end = datetime.now()
        start = end - timedelta(days=90)
        start_str = start.strftime("%Y-%m-%dT00:00:00")
        url = f"https://www.dallasopendata.com/resource/e7gq-4sah.csv?$where=issued_date>='{start_str}'&$limit=2000&$order=issued_date DESC"
        log.info("Dallas: fetching permits")
        r = requests.get(url, headers={"User-Agent": "CraneGeniusLeadBot/1.0"}, timeout=60)
        r.raise_for_status()
        df = pd.read_csv(io.StringIO(r.text), low_memory=False)
        log.info("Dallas: %d records", len(df))
        return df

    def parse(self, raw_df: pd.DataFrame) -> List[Dict[str, Any]]:
        if raw_df.empty:
            return []
        rows = []
        captured_at = utc_now_iso()
        for _, row in raw_df.iterrows():
            desc = normalize_text(str(row.get("work_description", "")))
            contractor = normalize_text(str(row.get("contractor", "")))
            if not desc and not contractor:
                continue
            rows.append({
                "source_id": self.source_id,
                "source_type": "permit",
                "jurisdiction": self.jurisdiction,
                "source_url": "https://www.dallasopendata.com/Services/Building-Permits/e7gq-4sah",
                "source_capture_utc": captured_at,
                "permit_or_record_id": normalize_text(str(row.get("permit_number", ""))),
                "record_status": "issued",
                "record_date": normalize_text(str(row.get("issued_date", ""))),
                "project_address": normalize_text(str(row.get("street_address", ""))),
                "project_city": "Dallas",
                "project_state": "TX",
                "contractor_name_raw": contractor,
                "description_raw": desc,
            })
        log.info("Dallas: %d usable rows", len(rows))
        return rows

    def run(self) -> List[Dict[str, Any]]:
        return self.parse(self.fetch())
