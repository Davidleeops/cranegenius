from __future__ import annotations
import io, logging
from datetime import datetime, timedelta
from typing import Any, Dict, List
import pandas as pd, requests
from ..utils import normalize_text, utc_now_iso

log = logging.getLogger("cranegenius.scrapers.chicago")

SOCRATA_URL = "https://data.cityofchicago.org/resource/ydr8-5enu.csv"

class ChicagoScraper:
    def __init__(self, source_config=None):
        self.source_id = (source_config or {}).get("id", "chicago_permits")
        self.jurisdiction = (source_config or {}).get("jurisdiction", "Chicago, IL")

    def fetch(self) -> pd.DataFrame:
        cutoff = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%dT%H:%M:%S.000")
        params = {
            "$limit": "2000",
            "$order": "reported_cost DESC",
            "$where": f"issue_date > '{cutoff}' AND permit_type like '%NEW%'",
        }
        log.info("Chicago: fetching permits from Socrata...")
        r = requests.get(SOCRATA_URL, params=params, headers={"User-Agent": "CraneGeniusLeadBot/1.0"}, timeout=60)
        r.raise_for_status()
        df = pd.read_csv(io.StringIO(r.text), low_memory=False)
        log.info("Chicago: %d records fetched", len(df))
        return df

    def parse(self, raw_df: pd.DataFrame) -> List[Dict[str, Any]]:
        if raw_df.empty:
            return []
        rows = []
        captured_at = utc_now_iso()
        for _, row in raw_df.iterrows():
            # Build address
            address = " ".join(filter(None, [
                str(row.get("street_number", "") or ""),
                str(row.get("street_direction", "") or ""),
                str(row.get("street_name", "") or ""),
            ])).strip()

            desc = normalize_text(str(row.get("work_description", "") or ""))
            cost = str(row.get("reported_cost", "") or "")

            # Extract GC from contacts
            contractor = ""
            for i in range(1, 16):
                ctype = str(row.get(f"contact_{i}_type", "") or "").upper()
                cname = str(row.get(f"contact_{i}_name", "") or "")
                if "GENERAL CONTRACTOR" in ctype and cname:
                    contractor = normalize_text(cname)
                    break
            # Fallback to any contractor
            if not contractor:
                for i in range(1, 16):
                    ctype = str(row.get(f"contact_{i}_type", "") or "").upper()
                    cname = str(row.get(f"contact_{i}_name", "") or "")
                    if "CONTRACTOR" in ctype and cname:
                        contractor = normalize_text(cname)
                        break

            if not desc and not contractor:
                continue

            # Add cost to description for scoring
            if cost:
                desc = f"{desc} REPORTED_COST:{cost}"

            rows.append({
                "source_id": self.source_id,
                "source_type": "permit",
                "jurisdiction": self.jurisdiction,
                "source_url": "https://data.cityofchicago.org/Buildings/Building-Permits/ydr8-5enu",
                "source_capture_utc": captured_at,
                "permit_or_record_id": str(row.get("permit_", "") or row.get("id", "")),
                "record_status": normalize_text(str(row.get("permit_status", "") or "issued")),
                "record_date": str(row.get("issue_date", "") or "")[:10],
                "project_address": address,
                "project_city": "Chicago",
                "project_state": "IL",
                "contractor_name_raw": contractor,
                "description_raw": desc,
            })

        log.info("Chicago: %d usable rows", len(rows))
        return rows

    def run(self) -> List[Dict[str, Any]]:
        return self.parse(self.fetch())
