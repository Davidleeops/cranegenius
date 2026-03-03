from __future__ import annotations

from .phoenix import PhoenixScraper
from .dallas import DallasScraper
from .chicago import ChicagoScraper

SCRAPER_REGISTRY = {
    "phoenix_csv": PhoenixScraper,
    "dallas_socrata": DallasScraper,
    "chicago_socrata": ChicagoScraper,
}

__all__ = ["SCRAPER_REGISTRY", "PhoenixScraper", "DallasScraper", "ChicagoScraper"]
