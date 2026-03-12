from __future__ import annotations
import logging
from typing import Any, Dict, List
import pandas as pd
from .utils import load_yaml, normalize_text

log = logging.getLogger("cranegenius.candidates")
MAX_CANDIDATES_PER_DOMAIN = 2
ROLE_INBOX_PRIORITY = ["estimating", "bids", "projects", "operations", "info"]

def build_candidates(enriched_df, keywords_yaml, contacts_df=None, patterns_df=None):
    cfg = load_yaml(keywords_yaml)
    configured = [normalize_text(x).lower().replace("@", "") for x in cfg.get("role_inboxes", []) if x]
    role_inboxes = ROLE_INBOX_PRIORITY + [x for x in configured if x and x not in ROLE_INBOX_PRIORITY]

    known_persons: Dict[str, List[str]] = {}
    if contacts_df is not None and not contacts_df.empty:
        pc = contacts_df[contacts_df.get("email_type","")=="person"] if "email_type" in contacts_df.columns else pd.DataFrame()
        for _, r in pc.iterrows():
            d = normalize_text(r.get("source_domain","")).lower()
            e = normalize_text(r.get("email","")).lower()
            if d and e:
                known_persons.setdefault(d, []).append(e)

    rows: List[Dict[str, Any]] = []
    seen_emails: set = set()
    seen_domains: set = set()

    for _, r in enriched_df.iterrows():
        raw_domain = normalize_text(r.get("contractor_domain","")).lower()
        if not raw_domain:
            continue
        domain_list = [d.strip() for d in raw_domain.split("|") if d.strip()]
        for domain in domain_list:
            if domain in seen_domains:
                continue
            seen_domains.add(domain)
            count = 0
            for prefix in role_inboxes:
                if count >= MAX_CANDIDATES_PER_DOMAIN:
                    break
                email = f"{prefix}@{domain}"
                if email not in seen_emails:
                    seen_emails.add(email)
                    rows.append({"contractor_domain": domain, "contractor_name_normalized": r.get("contractor_name_normalized",""), "jurisdiction": r.get("jurisdiction",""), "project_address": r.get("project_address",""), "score": r.get("score",0), "email_candidate": email, "contact_role_bucket": "role_inbox", "generation_method": "role_inbox"})
                    count += 1
            for email in known_persons.get(domain, []):
                if count >= MAX_CANDIDATES_PER_DOMAIN:
                    break
                if email not in seen_emails:
                    seen_emails.add(email)
                    rows.append({"contractor_domain": domain, "contractor_name_normalized": r.get("contractor_name_normalized",""), "jurisdiction": r.get("jurisdiction",""), "project_address": r.get("project_address",""), "score": r.get("score",0), "email_candidate": email, "contact_role_bucket": "person_discovered", "generation_method": "site_discovered"})
                    count += 1

    df = pd.DataFrame(rows)
    log.info("Candidates built: %d total across %d domains", len(df), len(seen_domains))
    return df
