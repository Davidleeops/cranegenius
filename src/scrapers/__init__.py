from __future__ import annotations

from .phoenix import PhoenixScraper

# Registry: source method name → scraper class
# Add new metros here as you build them
SCRAPER_REGISTRY = {
    "phoenix_csv": PhoenixScraper,
}

__all__ = ["SCRAPER_REGISTRY", "PhoenixScraper"]
