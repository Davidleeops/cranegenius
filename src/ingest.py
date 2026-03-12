from __future__ import annotations

import io
import logging
from typing import Any, Dict, List

import pandas as pd
import requests
from bs4 import BeautifulSoup

from .scrapers import SCRAPER_REGISTRY
from .scrapers.bid_board_scraper import BidBoardScraper
from .scrapers.contractor_directory_scraper import ContractorDirectoryScraper
from .scrapers.industrial_project_scraper import IndustrialProjectScraper
from .scrapers.permit_multi_city_scraper import PermitMultiCityScraper
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
    "project_cost_optional",
    "signal_keywords",
    "company_name",
    "project_description_optional",
    "city",
    "state",
    "capture_timestamp",
]

MULTI_SOURCE_SCRAPERS = [
    PermitMultiCityScraper,
    ContractorDirectoryScraper,
    BidBoardScraper,
    IndustrialProjectScraper,
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
            log.warning("Source %s failed (skipping): %s", source_id, exc)

    # Additive multi-source discovery layer for outbound scale.
    rows.extend(_ingest_multi_source_signals())

    df = pd.DataFrame(rows, columns=RAW_COLUMNS)
    log.info("Ingest complete: %d total raw rows", len(df))
    return df


def _ingest_multi_source_signals() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for scraper_cls in MULTI_SOURCE_SCRAPERS:
        scraper = scraper_cls({})
        try:
            signal_rows = scraper.run()
            for signal in signal_rows:
                rows.append(_signal_to_raw(scraper.source_id, signal))
        except Exception as exc:
            log.warning("Multi-source scraper %s failed: %s", scraper_cls.__name__, exc)
    log.info("Multi-source discovery added %d rows", len(rows))
    return rows


def _signal_to_raw(source_id: str, signal: Dict[str, Any]) -> Dict[str, Any]:
    city = normalize_text(signal.get("city"))
    state = normalize_text(signal.get("state"))
    capture_ts = normalize_text(signal.get("capture_timestamp")) or utc_now_iso()
    description = normalize_text(signal.get("project_description_optional"))
    company = normalize_text(signal.get("company_name"))
    signal_keywords = signal.get("signal_keywords", "")
    if isinstance(signal_keywords, list):
        signal_keywords = ",".join([normalize_text(x) for x in signal_keywords if normalize_text(x)])
    signal_keywords = normalize_text(signal_keywords)

    return {
        "source_id": source_id,
        "source_type": normalize_text(signal.get("source_type")) or "signal",
        "jurisdiction": ", ".join([x for x in [city, state] if x]),
        "source_url": normalize_text(signal.get("source_url")),
        "source_capture_utc": capture_ts,
        "permit_or_record_id": "",
        "record_status": "detected",
        "record_date": capture_ts[:10],
        "project_address": "",
        "project_city": city,
        "project_state": state,
        "contractor_name_raw": company,
        "description_raw": description,
        "project_cost_optional": signal.get("project_cost_optional", ""),
        "signal_keywords": signal_keywords,
        "company_name": company,
        "project_description_optional": description,
        "city": city,
        "state": state,
        "capture_timestamp": capture_ts,
    }


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
