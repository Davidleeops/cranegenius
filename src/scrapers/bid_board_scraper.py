from __future__ import annotations

import logging
from typing import Any, Dict, List

import requests
from bs4 import BeautifulSoup

from ..utils import normalize_text, utc_now_iso

log = logging.getLogger("cranegenius.scrapers.bid_board")


class BidBoardScraper:
    def __init__(self, source_config: Dict[str, Any] | None = None):
        cfg = source_config or {}
        self.source_id = cfg.get("id", "bid_board")
        self.source_type = "public_bid"
        self.url = cfg.get("url", "https://sam.gov/")

    def _seed(self) -> List[Dict[str, Any]]:
        ts = utc_now_iso()
        return [
            {
                "source_type": self.source_type,
                "company_name": "Metro Civil + Crane JV",
                "project_description_optional": "Municipal bridge replacement with heavy picks and night closures",
                "city": "Atlanta",
                "state": "GA",
                "project_cost_optional": 46000000,
                "signal_keywords": "bridge,infrastructure,heavy lift,night work",
                "source_url": self.url,
                "capture_timestamp": ts,
            },
            {
                "source_type": self.source_type,
                "company_name": "North Coast Infrastructure Partners",
                "project_description_optional": "Utility substation modernization and transformer placement",
                "city": "Cleveland",
                "state": "OH",
                "project_cost_optional": 28000000,
                "signal_keywords": "substation,transformer,utility,heavy haul",
                "source_url": self.url,
                "capture_timestamp": ts,
            },
        ]

    def _parse_html(self, html: str) -> List[Dict[str, Any]]:
        soup = BeautifulSoup(html, "lxml")
        rows: List[Dict[str, Any]] = []
        ts = utc_now_iso()
        for item in soup.select("article, .opportunity, .listing"):
            text = normalize_text(item.get_text(" ", strip=True))
            if not text:
                continue
            rows.append(
                {
                    "source_type": self.source_type,
                    "company_name": normalize_text(item.get("data-company", "")) or "Unknown Bid Contractor",
                    "project_description_optional": text[:400],
                    "city": normalize_text(item.get("data-city", "")),
                    "state": normalize_text(item.get("data-state", "")),
                    "project_cost_optional": "",
                    "signal_keywords": "bid board,public project",
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
                    log.info("bid_board: parsed %d rows", len(parsed))
                    return parsed
        except Exception as exc:
            log.debug("bid_board fetch failed: %s", exc)
        seed = self._seed()
        log.info("bid_board: using %d seed rows", len(seed))
        return seed
