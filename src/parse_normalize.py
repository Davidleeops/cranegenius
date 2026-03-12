from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Tuple

import pandas as pd
from dateutil import parser as dateparser

from .utils import normalize_text, sha1

log = logging.getLogger("cranegenius.parse")
COST_M_RE = re.compile(r"(\d+(?:\.\d+)?)\s*m(?:illion)?\b", re.IGNORECASE)
COST_PLAIN_RE = re.compile(r"\$?\s*([0-9]{1,3}(?:,[0-9]{3})+|[0-9]{7,})")
COMPANY_PHONE_RE = re.compile(r"\(?\b\d{3}\)?[\s\-.]?\d{3}[\s\-.]?\d{4}\b|\b\d{10}\b")
COMPANY_ADDRESS_START_RE = re.compile(
    r"\b\d{1,6}\s+[a-z0-9.\-]+(?:\s+[a-z0-9.\-]+){0,6}\s+"
    r"(?:st|street|rd|road|ave|avenue|blvd|boulevard|ln|lane|dr|drive|pkwy|parkway|hwy|highway|ct|court|cir|circle|way|pl|place|trl|trail)\b",
    re.IGNORECASE,
)
COMPANY_CITY_STATE_ZIP_TAIL_RE = re.compile(r",?\s*[a-z .'-]+,\s*[a-z]{2}\s+\d{5}(?:-\d{4})?\s*$", re.IGNORECASE)
COMPANY_SUFFIX_NOISE_RE = re.compile(r"[\\/]+\s*(?:\d|po\s*box|p\.?o\.?\s*box|[a-z]{2}\b|contact|locations?)\b.*$", re.IGNORECASE)

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
    "project_cost_optional",
    "signal_keywords",
    "contractor_name_raw",
    "contractor_name_normalized",
    "dedupe_key",
]


def clean_contractor_name(value: Any) -> str:
    raw = normalize_text(value)
    if not raw:
        return ""
    text = raw.lower()
    text = COMPANY_PHONE_RE.sub(" ", text)
    text = COMPANY_SUFFIX_NOISE_RE.sub(" ", text)
    addr = COMPANY_ADDRESS_START_RE.search(text)
    if addr:
        text = text[: addr.start()]
    text = COMPANY_CITY_STATE_ZIP_TAIL_RE.sub(" ", text)
    text = re.sub(r"\s+", " ", text).strip(" ,.-/\\")
    return text


def _parse_cost(value: Any, desc: str) -> float:
    raw = normalize_text(value)
    if raw:
        try:
            return float(str(raw).replace("$", "").replace(",", "").strip())
        except Exception:
            pass
    text = normalize_text(desc)
    m = COST_M_RE.search(text)
    if m:
        try:
            return float(m.group(1)) * 1_000_000
        except Exception:
            return 0.0
    m2 = COST_PLAIN_RE.search(text)
    if m2:
        try:
            return float(m2.group(1).replace(",", ""))
        except Exception:
            return 0.0
    return 0.0


def normalize_records(raw_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    rows: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    for _, r in raw_df.iterrows():
        desc = normalize_text(r.get("description_raw") or r.get("project_description_optional"))
        contractor_raw = normalize_text(r.get("contractor_name_raw") or r.get("company_name"))
        contractor_norm = clean_contractor_name(contractor_raw)

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

        state = normalize_text(r.get("project_state") or r.get("state"))
        city = normalize_text(r.get("project_city") or r.get("city"))
        addr = normalize_text(r.get("project_address"))
        signal_keywords = normalize_text(r.get("signal_keywords"))
        project_cost = _parse_cost(r.get("project_cost_optional"), desc)

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
            "project_cost_optional": int(project_cost),
            "signal_keywords": signal_keywords,
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
