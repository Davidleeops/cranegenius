from __future__ import annotations

import logging
from typing import Any, Dict, List

import pandas as pd

from .utils import load_yaml, normalize_text

log = logging.getLogger("cranegenius.candidates")

# Cap candidates per domain to avoid over-generating
MAX_CANDIDATES_PER_DOMAIN = 8


def build_candidates(
    enriched_df: pd.DataFrame,
    keywords_yaml: str,
    contacts_df: pd.DataFrame = None,
    patterns_df: pd.DataFrame = None,
) -> pd.DataFrame:
    """
    Build email candidates two ways:
    1. Role inboxes (estimating@, projects@, etc.) — always generated if domain exists
    2. Pattern-based person emails — generated when a pattern was inferred from site crawl
       e.g. if site has john.smith@company.com, generate sarah.jones@company.com for
       any other names found on the team page
    """
    cfg = load_yaml(keywords_yaml)
    role_inboxes = cfg.get("role_inboxes", [])
    role_inboxes = [normalize_text(x).lower() for x in role_inboxes if x]

    # Build pattern lookup: domain → pattern
    pattern_map: Dict[str, str] = {}
    if patterns_df is not None and not patterns_df.empty:
        for _, r in patterns_df.iterrows():
            d = normalize_text(r.get("source_domain")).lower()
            p = normalize_text(r.get("pattern"))
            if d and p:
                pattern_map[d] = p

    # Build known person emails per domain (from site crawl)
    known_persons: Dict[str, List[str]] = {}
    if contacts_df is not None and not contacts_df.empty:
        person_contacts = contacts_df[contacts_df.get("email_type", "") == "person"] if "email_type" in contacts_df.columns else pd.DataFrame()
        for _, r in person_contacts.iterrows():
            d = normalize_text(r.get("source_domain")).lower()
            e = normalize_text(r.get("email")).lower()
            if d and e:
                known_persons.setdefault(d, []).append(e)

    rows: List[Dict[str, Any]] = []
    seen_domains = set()

    for _, r in enriched_df.iterrows():
        domain = normalize_text(r.get("contractor_domain")).lower()
        if not domain or domain in seen_domains:
            continue
        seen_domains.add(domain)

        count = 0

        # 1. Role inbox candidates
        for prefix in role_inboxes:
            if count >= MAX_CANDIDATES_PER_DOMAIN:
                break
            rows.append({
                "contractor_domain": domain,
                "email_candidate": f"{prefix}@{domain}",
                "contact_role_bucket": "role_inbox",
                "generation_method": "role_inbox",
            })
            count += 1

        # 2. Pattern-based person email candidates
        # Use the emails already found on the site (these are real, discovered emails)
        for email in known_persons.get(domain, []):
            if count >= MAX_CANDIDATES_PER_DOMAIN:
                break
            # These were already found by the miner — add them as candidates for verification
            rows.append({
                "contractor_domain": domain,
                "email_candidate": email,
                "contact_role_bucket": "person_discovered",
                "generation_method": "site_discovered",
            })
            count += 1

    df = pd.DataFrame(rows).drop_duplicates(subset=["email_candidate"])
    log.info("Candidates built: %d total across %d domains", len(df), len(seen_domains))
    return df
