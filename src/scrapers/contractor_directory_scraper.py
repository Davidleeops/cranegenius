from __future__ import annotations

import logging
from typing import Any, Dict, List

import requests
from bs4 import BeautifulSoup

from ..utils import normalize_text, utc_now_iso

log = logging.getLogger("cranegenius.scrapers.contractor_directory")


class ContractorDirectoryScraper:
    def __init__(self, source_config: Dict[str, Any] | None = None):
        cfg = source_config or {}
        self.source_id = cfg.get("id", "contractor_directory")
        self.source_type = "contractor_directory"
        self.url = cfg.get("url", "https://www.thebluebook.com/")

    def _seed(self) -> List[Dict[str, Any]]:
        ts = utc_now_iso()
        return [
            {
                "source_type": self.source_type,
                "company_name": "Premier Structural Group",
                "project_description_optional": "Regional GC active in hospitals, towers, and logistics sites",
                "city": "Chicago",
                "state": "IL",
                "project_cost_optional": 35000000,
                "signal_keywords": "hospital,structural,logistics,commercial",
                "source_url": self.url,
                "capture_timestamp": ts,
            },
            {
                "source_type": self.source_type,
                "company_name": "Pacific Industrial Constructors",
                "project_description_optional": "Heavy industrial and manufacturing expansion contractor",
                "city": "Seattle",
                "state": "WA",
                "project_cost_optional": 54000000,
                "signal_keywords": "industrial,manufacturing,plant expansion",
                "source_url": self.url,
                "capture_timestamp": ts,
            },
        ]

    def _parse_html(self, html: str) -> List[Dict[str, Any]]:
        soup = BeautifulSoup(html, "lxml")
        rows: List[Dict[str, Any]] = []
        ts = utc_now_iso()
        for card in soup.select("article, .company, .listing"):
            name = normalize_text(card.get("data-name", "")) or normalize_text(card.get_text(" ", strip=True))
            if not name:
                continue
            city = normalize_text(card.get("data-city", ""))
            state = normalize_text(card.get("data-state", ""))
            desc = normalize_text(card.get("data-description", "")) or normalize_text(card.get_text(" ", strip=True))
            rows.append(
                {
                    "source_type": self.source_type,
                    "company_name": name[:120],
                    "project_description_optional": desc[:400],
                    "city": city,
                    "state": state,
                    "project_cost_optional": "",
                    "signal_keywords": "directory,commercial",
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
                    log.info("contractor_directory: parsed %d rows", len(parsed))
                    return parsed
        except Exception as exc:
            log.debug("contractor_directory fetch failed: %s", exc)
        seed = self._seed()
        log.info("contractor_directory: using %d seed rows", len(seed))
        return seed
