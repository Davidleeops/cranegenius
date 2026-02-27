from __future__ import annotations

import logging
import re
from typing import Dict, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

from .utils import normalize_text

log = logging.getLogger("cranegenius.resolver")

# Arizona ROC license search — public, no auth required
AZ_ROC_SEARCH_URL = "https://roc.az.gov/Lookup/LicenseLookup"

# Simple domain pattern for extracting from text
DOMAIN_RE = re.compile(r'\b(?:www\.)?([a-zA-Z0-9\-]+\.[a-zA-Z]{2,})\b')


def resolve_domains(scored_df: pd.DataFrame, seed_path: str = "data/company_domain_seed.csv") -> pd.DataFrame:
    """
    Resolve contractor_name_normalized → contractor_domain.
    
    Strategy:
    1. Check local seed CSV (fast, reliable)
    2. Try AZ ROC registry lookup (for Arizona contractors)
    3. Leave blank if neither works (manual review queue)
    """
    seed_map = _load_seed(seed_path)
    log.info("Seed map loaded: %d entries", len(seed_map))

    out = scored_df.copy()
    domains = []
    resolution_sources = []

    for _, row in out.iterrows():
        name = normalize_text(row.get("contractor_name_normalized")).lower()
        state = normalize_text(row.get("project_state")).upper()

        domain, source = _resolve_one(name, state, seed_map)
        domains.append(domain)
        resolution_sources.append(source)

    out["contractor_domain"] = domains
    out["domain_resolution_source"] = resolution_sources

    resolved = sum(1 for d in domains if d)
    total = len(domains)
    pct = (resolved / total * 100) if total else 0
    log.info("Domain resolution: %d/%d resolved (%.1f%%)", resolved, total, pct)

    if total > 0 and pct < 25:
        log.warning(
            "GATE WARNING: Domain resolution rate %.1f%% is below 25%%. "
            "Build up company_domain_seed.csv before scaling sends.",
            pct
        )

    return out


def _load_seed(seed_path: str) -> Dict[str, str]:
    seed_map: Dict[str, str] = {}
    try:
        df = pd.read_csv(seed_path)
        for _, r in df.iterrows():
            k = normalize_text(r.get("contractor_name_normalized")).lower().strip()
            v = normalize_text(r.get("contractor_domain")).lower().strip()
            if k and v:
                seed_map[k] = v
                # Also index without common suffixes for fuzzy matching
                for suffix in [" llc", " inc", " corp", " co", " company"]:
                    cleaned = k.replace(suffix, "").strip()
                    if cleaned and cleaned not in seed_map:
                        seed_map[cleaned] = v
    except FileNotFoundError:
        log.warning("Seed file not found: %s — domain resolution will be limited", seed_path)
    except Exception as exc:
        log.error("Error loading seed: %s", exc)
    return seed_map


def _resolve_one(name: str, state: str, seed_map: Dict[str, str]) -> tuple[str, str]:
    """Returns (domain, source_label)."""
    if not name:
        return "", "none"

    # 1. Direct seed lookup
    if name in seed_map:
        return seed_map[name], "seed"

    # 2. Partial seed match (name is a substring of a seed key)
    for seed_key, domain in seed_map.items():
        if name and len(name) > 5 and name in seed_key:
            return domain, "seed_partial"

    # 3. AZ ROC fallback for Arizona contractors
    if state == "AZ":
        domain = _try_az_roc(name)
        if domain:
            return domain, "az_roc"

    return "", "unresolved"


def _try_az_roc(contractor_name: str) -> Optional[str]:
    """
    Attempt to find contractor website via Arizona ROC license search.
    This is a best-effort lookup — ROC doesn't always have websites.
    """
    try:
        # Search the ROC license lookup
        search_name = contractor_name[:40]  # ROC search has length limits
        params = {
            "licenseNumber": "",
            "companyName": search_name,
            "licenseType": "",
            "city": "",
            "county": "",
        }
        headers = {
            "User-Agent": "CraneGeniusLeadBot/1.0",
            "Referer": AZ_ROC_SEARCH_URL,
        }
        r = requests.get(AZ_ROC_SEARCH_URL, params=params, headers=headers, timeout=15)
        if r.status_code != 200:
            return None

        soup = BeautifulSoup(r.text, "lxml")

        # Look for website fields in the result — ROC pages vary
        # Try common patterns
        for a in soup.select("a[href]"):
            href = a.get("href", "")
            if href.startswith("http") and "az.gov" not in href and "roc.az" not in href:
                # Looks like a contractor website link
                from .utils import domain_from_url
                d = domain_from_url(href)
                if d and len(d) > 4:
                    log.info("AZ ROC found domain for '%s': %s", contractor_name[:30], d)
                    return d

        return None

    except Exception as exc:
        log.debug("AZ ROC lookup failed for '%s': %s", contractor_name[:30], exc)
        return None
