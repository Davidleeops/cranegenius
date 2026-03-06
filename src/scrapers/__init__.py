from __future__ import annotations
from .phoenix import PhoenixScraper
from .dallas import DallasScraper
from .chicago import ChicagoScraper
from .nyc import NYCScraper

SCRAPER_REGISTRY = {
    "phoenix_csv": PhoenixScraper,
    "dallas_socrata": DallasScraper,
    "chicago_socrata": ChicagoScraper,
    "nyc_socrata": NYCScraper,
}

__all__ = ["SCRAPER_REGISTRY", "PhoenixScraper", "DallasScraper", "ChicagoScraper", "NYCScraper"]
