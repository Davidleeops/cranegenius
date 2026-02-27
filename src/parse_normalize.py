from __future__ import annotations

import logging
from typing import Any, Dict, List, Tuple

import pandas as pd
from dateutil import parser as dateparser

from .utils import normalize_text, sha1

log = logging.getLogger("cranegenius.parse")

NORMALIZED_COLUMNS = [
    "source_type",
    "source_url",
    "source_capture_utc",
    "jurisdiction",
    "permit_or_record_id",
    "record_status",
    "record_date",
    "record_date_iso",
    "project_address",
    "project_city",
    "project_state",
    "description_raw",
    "contractor_name_raw",
    "contractor_name_normalized",
    "dedupe_key",
]


def normalize_records(raw_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    rows: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    for _, r in raw_df.iterrows():
        desc = normalize_text(r.get("description_raw"))
        contractor_raw = normalize_text(r.get("contractor_name_raw"))
        contractor_norm = contractor_raw.lower().strip()

        # Strip common legal suffixes for better seed matching
        for suffix in [" llc", " inc", " corp", " co.", " company", " ltd", " lp", " lllp"]:
            contractor_norm = contractor_norm.replace(suffix, "").strip()

        date_raw = normalize_text(r.get("record_date"))
        date_iso = ""
        if date_raw:
            try:
                dt = dateparser.parse(date_raw, fuzzy=True)
                date_iso = dt.date().isoformat()
            except Exception:
                errors.append({
                    "type": "date_parse",
                    "value": date_raw,
                    "source_url": r.get("source_url", ""),
                })

        state = normalize_text(r.get("project_state"))
        city = normalize_text(r.get("project_city"))
        addr = normalize_text(r.get("project_address"))

        dedupe = sha1("|".join([
            normalize_text(r.get("source_type")),
            normalize_text(r.get("permit_or_record_id")),
            addr.lower(),
            contractor_norm,
            (date_iso or date_raw).lower(),
            desc.lower()[:120],
        ]))

        rows.append({
            "source_type": normalize_text(r.get("source_type")),
            "source_url": normalize_text(r.get("source_url")),
            "source_capture_utc": normalize_text(r.get("source_capture_utc")),
            "jurisdiction": normalize_text(r.get("jurisdiction")),
            "permit_or_record_id": normalize_text(r.get("permit_or_record_id")),
            "record_status": normalize_text(r.get("record_status")).lower(),
            "record_date": date_raw,
            "record_date_iso": date_iso,
            "project_address": addr,
            "project_city": city,
            "project_state": state,
            "description_raw": desc,
            "contractor_name_raw": contractor_raw,
            "contractor_name_normalized": contractor_norm,
            "dedupe_key": dedupe,
        })

    before = len(rows)
    df = pd.DataFrame(rows, columns=NORMALIZED_COLUMNS).drop_duplicates(subset=["dedupe_key"])
    after = len(df)
    log.info("Normalized: %d rows in, %d after dedup (%d dupes removed)", before, after, before - after)

    err = pd.DataFrame(errors)
    if len(errors):
        log.warning("Parse errors: %d date parse failures", len(errors))

    return df, err
