from __future__ import annotations

import io
import logging
from typing import Any, Dict, List

import pandas as pd
import requests
from bs4 import BeautifulSoup

from .scrapers import SCRAPER_REGISTRY
from .utils import load_yaml, normalize_text, utc_now_iso

log = logging.getLogger("cranegenius.ingest")

RAW_COLUMNS = [
    "source_id",
    "source_type",
    "jurisdiction",
    "source_url",
    "source_capture_utc",
    "permit_or_record_id",
    "record_status",
    "record_date",
    "project_address",
    "project_city",
    "project_state",
    "contractor_name_raw",
    "description_raw",
]


def ingest_sources(sources_yaml_path: str) -> pd.DataFrame:
    cfg = load_yaml(sources_yaml_path)
    sources = [s for s in cfg.get("sources", []) if s.get("enabled")]

    if not sources:
        log.warning("No enabled sources found in %s", sources_yaml_path)
        return pd.DataFrame(columns=RAW_COLUMNS)

    log.info("Ingesting %d enabled source(s)...", len(sources))
    rows: List[Dict[str, Any]] = []

    for s in sources:
        method = s.get("method", "")
        source_id = s.get("id", "unknown")
        log.info("  → source: %s (method: %s)", source_id, method)

        try:
            # Check registry first (metro-specific scrapers)
            if method in SCRAPER_REGISTRY:
                scraper = SCRAPER_REGISTRY[method](s)
                rows.extend(scraper.run())

            elif method == "csv":
                rows.extend(_ingest_generic_csv(s))

            elif method == "html_list":
                rows.extend(_ingest_html_list_basic(s))

            else:
                log.warning("Unknown method '%s' for source %s — skipping", method, source_id)

        except Exception as exc:
            log.error("Source %s failed: %s", source_id, exc, exc_info=True)

    df = pd.DataFrame(rows, columns=RAW_COLUMNS)
    log.info("Ingest complete: %d total raw rows", len(df))
    return df


def _ingest_generic_csv(source: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Generic CSV ingest — tries common field name patterns."""
    url = source["url"]
    r = requests.get(url, timeout=30, headers={"User-Agent": "CraneGeniusLeadBot/1.0"})
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text), low_memory=False)

    def pick(row: Any, *keys: str) -> str:
        for k in keys:
            v = row.get(k)
            if v and str(v).strip():
                return normalize_text(v)
        return ""

    rows = []
    for _, row in df.iterrows():
        rows.append({
            "source_id": source["id"],
            "source_type": source["source_type"],
            "jurisdiction": source["jurisdiction"],
            "source_url": url,
            "source_capture_utc": utc_now_iso(),
            "permit_or_record_id": pick(row, "permit_id", "PermitNumber", "id", "record_id"),
            "record_status": pick(row, "status", "StatusCurrent", "permit_status"),
            "record_date": pick(row, "issued_date", "IssuedDate", "date", "applied_date"),
            "project_address": pick(row, "address", "SiteAddress", "project_address"),
            "project_city": pick(row, "city", "SiteCity", "project_city"),
            "project_state": pick(row, "state", "SiteState", "project_state"),
            "contractor_name_raw": pick(row, "contractor", "ContractorName", "contractor_name"),
            "description_raw": pick(row, "description", "ProjectDescription", "work_description"),
        })
    return rows


def _ingest_html_list_basic(source: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Basic HTML table scraper — works for simple permit table pages."""
    url = source["url"]
    r = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")

    rows = []
    for item in soup.select("table tr"):
        tds = [normalize_text(td.get_text(" ", strip=True)) for td in item.select("td")]
        if len(tds) < 4:
            continue
        rows.append({
            "source_id": source["id"],
            "source_type": source["source_type"],
            "jurisdiction": source["jurisdiction"],
            "source_url": url,
            "source_capture_utc": utc_now_iso(),
            "permit_or_record_id": tds[0],
            "record_status": tds[1],
            "record_date": tds[2],
            "project_address": tds[3],
            "project_city": "",
            "project_state": "",
            "contractor_name_raw": "",
            "description_raw": " ".join(tds[4:]) if len(tds) > 4 else "",
        })
    return rows
