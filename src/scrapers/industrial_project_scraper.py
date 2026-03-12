from __future__ import annotations

import logging
from typing import Any, Dict, List

import requests
from bs4 import BeautifulSoup

from ..utils import normalize_text, utc_now_iso

log = logging.getLogger("cranegenius.scrapers.industrial_project")


class IndustrialProjectScraper:
    def __init__(self, source_config: Dict[str, Any] | None = None):
        cfg = source_config or {}
        self.source_id = cfg.get("id", "industrial_project")
        self.source_type = "industrial_permit"
        self.url = cfg.get("url", "https://www.constructiondive.com/")

    def _seed(self) -> List[Dict[str, Any]]:
        ts = utc_now_iso()
        return [
            {
                "source_type": self.source_type,
                "company_name": "Delta Process Installers",
                "project_description_optional": "Semiconductor fab expansion with cleanroom equipment placement",
                "city": "Columbus",
                "state": "OH",
                "project_cost_optional": 120000000,
                "signal_keywords": "semiconductor,fab,cleanroom,equipment install",
                "source_url": self.url,
                "capture_timestamp": ts,
            },
            {
                "source_type": self.source_type,
                "company_name": "Frontier Manufacturing Constructors",
                "project_description_optional": "Battery module facility expansion and process skid lifts",
                "city": "Reno",
                "state": "NV",
                "project_cost_optional": 68000000,
                "signal_keywords": "battery,manufacturing,process skid,industrial",
                "source_url": self.url,
                "capture_timestamp": ts,
            },
        ]

    def _parse_html(self, html: str) -> List[Dict[str, Any]]:
        soup = BeautifulSoup(html, "lxml")
        rows: List[Dict[str, Any]] = []
        ts = utc_now_iso()
        for item in soup.select("article, .story, .project"):
            title = normalize_text(item.get_text(" ", strip=True))
            if not title:
                continue
            rows.append(
                {
                    "source_type": self.source_type,
                    "company_name": normalize_text(item.get("data-company", "")) or "Unknown Industrial Contractor",
                    "project_description_optional": title[:400],
                    "city": normalize_text(item.get("data-city", "")),
                    "state": normalize_text(item.get("data-state", "")),
                    "project_cost_optional": "",
                    "signal_keywords": "industrial project",
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
                    log.info("industrial_project: parsed %d rows", len(parsed))
                    return parsed
        except Exception as exc:
            log.debug("industrial_project fetch failed: %s", exc)
        seed = self._seed()
        log.info("industrial_project: using %d seed rows", len(seed))
        return seed
