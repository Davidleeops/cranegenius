from __future__ import annotations
import io, logging
from datetime import datetime, timedelta
from typing import Any, Dict, List
import pandas as pd, requests
from ..utils import normalize_text, utc_now_iso

log = logging.getLogger("cranegenius.scrapers.nyc")
SOCRATA_URL = "https://data.cityofnewyork.us/resource/ipu4-2q9a.csv"

class NYCScraper:
    def __init__(self, source_config=None):
        self.source_id = (source_config or {}).get("id", "nyc_permits")
        self.jurisdiction = (source_config or {}).get("jurisdiction", "New York, NY")

    def fetch(self) -> pd.DataFrame:
        cutoff = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%dT%H:%M:%S.000")
        params = {
            "$limit": "5000",
            "$order": "issuance_date DESC",
            "$where": "job_type in('NB','A1') AND permittee_s_business_name is not null",
        }
        log.info("NYC: fetching permits from Socrata...")
        r = requests.get(SOCRATA_URL, params=params, headers={"User-Agent": "CraneGeniusLeadBot/1.0"}, timeout=60)
        r.raise_for_status()
        df = pd.read_csv(io.StringIO(r.text), low_memory=False)
        log.info("NYC: %d records fetched", len(df))
        return df

    def parse(self, raw_df: pd.DataFrame) -> List[Dict[str, Any]]:
        if raw_df.empty:
            return []
        rows = []
        captured_at = utc_now_iso()
        for _, row in raw_df.iterrows():
            address = " ".join(filter(None, [
                str(row.get("house__", "") or ""),
                str(row.get("street_name", "") or ""),
                str(row.get("borough", "") or ""),
            ])).strip()

            contractor = normalize_text(str(row.get("permittee_s_business_name", "") or ""))
            contact_first = str(row.get("permittee_s_first_name", "") or "").strip()
            contact_last = str(row.get("permittee_s_last_name", "") or "").strip()
            contact_name = f"{contact_first} {contact_last}".strip()
            phone = str(row.get("permittee_s_phone__", "") or "").strip()
            owner = normalize_text(str(row.get("owner_s_business_name", "") or ""))
            work_type = str(row.get("work_type", "") or "")
            job_type = str(row.get("job_type", "") or "")

            desc = f"{job_type} {work_type}".strip()
            if owner:
                desc += f" OWNER:{owner}"
            if contact_name:
                desc += f" CONTACT:{contact_name}"
            if phone:
                desc += f" PHONE:{phone}"

            if not contractor and not owner:
                continue

            rows.append({
                "source_id": self.source_id,
                "source_type": "permit",
                "jurisdiction": self.jurisdiction,
                "source_url": "https://data.cityofnewyork.us/Housing-Development/DOB-Permit-Issuance/ipu4-2q9a",
                "source_capture_utc": captured_at,
                "permit_or_record_id": str(row.get("job__", "") or ""),
                "record_status": normalize_text(str(row.get("permit_status", "") or "issued")),
                "record_date": str(row.get("issuance_date", "") or "")[:10],
                "project_address": address,
                "project_city": "New York",
                "project_state": "NY",
                "contractor_name_raw": f"{contractor} {phone}".strip() if phone else contractor,
                "description_raw": desc,
            })

        log.info("NYC: %d usable rows", len(rows))
        return rows

    def run(self) -> List[Dict[str, Any]]:
        return self.parse(self.fetch())
