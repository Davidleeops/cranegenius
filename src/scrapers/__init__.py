from __future__ import annotations
from .phoenix import PhoenixScraper
from .dallas import DallasScraper
from .chicago import ChicagoScraper
from .nyc import NYCScraper
from .permit_multi_city_scraper import PermitMultiCityScraper
from .contractor_directory_scraper import ContractorDirectoryScraper
from .bid_board_scraper import BidBoardScraper
from .industrial_project_scraper import IndustrialProjectScraper

SCRAPER_REGISTRY = {
    "phoenix_csv": PhoenixScraper,
    "dallas_socrata": DallasScraper,
    "chicago_socrata": ChicagoScraper,
    "nyc_socrata": NYCScraper,
    "permit_multi_city": PermitMultiCityScraper,
    "contractor_directory": ContractorDirectoryScraper,
    "bid_board": BidBoardScraper,
    "industrial_project": IndustrialProjectScraper,
}

__all__ = [
    "SCRAPER_REGISTRY",
    "PhoenixScraper",
    "DallasScraper",
    "ChicagoScraper",
    "NYCScraper",
    "PermitMultiCityScraper",
    "ContractorDirectoryScraper",
    "BidBoardScraper",
    "IndustrialProjectScraper",
]
