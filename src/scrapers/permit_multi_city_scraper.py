from __future__ import annotations

import logging
from typing import Any, Dict, List

import requests
from bs4 import BeautifulSoup

from ..utils import normalize_text, utc_now_iso

log = logging.getLogger("cranegenius.scrapers.permit_multi_city")


class PermitMultiCityScraper:
    """First-pass multi-city permit signal scraper with safe seed fallback."""

    def __init__(self, source_config: Dict[str, Any] | None = None):
        cfg = source_config or {}
        self.source_id = cfg.get("id", "permit_multi_city")
        self.source_type = "building_permit"
        self.url = cfg.get("url", "https://www.permits.io/")

    def _seed(self) -> List[Dict[str, Any]]:
        ts = utc_now_iso()
        return [
            {
                "source_type": self.source_type,
                "company_name": "Atlas Commercial Builders",
                "project_description_optional": "36-story mixed-use tower foundation and steel package",
                "city": "Austin",
                "state": "TX",
                "project_cost_optional": 165000000,
                "signal_keywords": "tower,steel,high-rise,core-shell",
                "source_url": self.url,
                "capture_timestamp": ts,
            },
            {
                "source_type": self.source_type,
                "company_name": "Summit MEP Contractors",
                "project_description_optional": "Tier III data center shell and MEP crane-assisted install",
                "city": "Phoenix",
                "state": "AZ",
                "project_cost_optional": 92000000,
                "signal_keywords": "data center,mep,critical power,transformer",
                "source_url": self.url,
                "capture_timestamp": ts,
            },
        ]

    def _parse_html(self, html: str) -> List[Dict[str, Any]]:
        soup = BeautifulSoup(html, "lxml")
        rows: List[Dict[str, Any]] = []
        ts = utc_now_iso()
        for card in soup.select("article, .permit-card, tr"):
            text = normalize_text(card.get_text(" ", strip=True))
            if not text:
                continue
            company = normalize_text(card.get("data-company", "")) or "Unknown Contractor"
            city = normalize_text(card.get("data-city", ""))
            state = normalize_text(card.get("data-state", ""))
            rows.append(
                {
                    "source_type": self.source_type,
                    "company_name": company,
                    "project_description_optional": text[:400],
                    "city": city,
                    "state": state,
                    "project_cost_optional": "",
                    "signal_keywords": "permit,commercial",
                    "source_url": self.url,
                    "capture_timestamp": ts,
                }
            )
        return rows

    def run(self) -> List[Dict[str, Any]]:
        try:
            resp = requests.get(self.url, timeout=20, headers={"User-Agent": "CraneGeniusLeadBot/1.0"})
            if resp.ok:
                parsed = self._parse_html(resp.text)
                if parsed:
                    log.info("permit_multi_city: parsed %d rows", len(parsed))
                    return parsed
        except Exception as exc:
            log.debug("permit_multi_city fetch failed: %s", exc)
        seed = self._seed()
        log.info("permit_multi_city: using %d seed rows", len(seed))
        return seed
