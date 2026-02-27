from __future__ import annotations

from .phoenix import PhoenixScraper
from .dallas import DallasScraper

# Registry: source method name → scraper class
SCRAPER_REGISTRY = {
    "phoenix_csv": PhoenixScraper,
    "dallas_socrata": DallasScraper,
}

__all__ = ["SCRAPER_REGISTRY", "PhoenixScraper", "DallasScraper"]
