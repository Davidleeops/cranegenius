from __future__ import annotations

import logging
import re
from collections import deque
from typing import Any, Dict, List, Set, Tuple
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup

from .utils import extract_emails, extract_phones, load_yaml, normalize_text, rate_limit_sleep, utc_now_iso

log = logging.getLogger("cranegenius.miner")

ROLE_INBOX_PREFIXES = {
    "info", "estimating", "estimates", "bids", "projects", "project",
    "operations", "ops", "dispatch", "safety", "construction", "contact",
    "admin", "office",
}

# Patterns that suggest a person name email (not a role inbox)
PERSON_EMAIL_RE = re.compile(r"^[a-z]+\.[a-z]+@|^[a-z]\.[a-z]+@|^[a-z]+_[a-z]+@")


def mine_contacts(enriched_df: pd.DataFrame, crawler_yaml: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    cfg = load_yaml(crawler_yaml)["crawler"]
    include_keywords = [k.lower() for k in cfg["include_url_keywords"]]
    max_pages = int(cfg["max_pages_per_domain"])
    max_depth = int(cfg["max_depth"])
    timeout = int(cfg["request_timeout_seconds"])
    rate_s = float(cfg["rate_limit_seconds"])
    ua = cfg["user_agent"]
    exclude_exts = [e.lower() for e in cfg.get("exclude_extensions", [])]

    contacts_rows: List[Dict[str, Any]] = []
    patterns_rows: List[Dict[str, Any]] = []

    # Dedupe by domain — only crawl each domain once
    seen_domains: Set[str] = set()

    for _, row in enriched_df.iterrows():
        domain = normalize_text(row.get("contractor_domain")).lower()
        if not domain or domain in seen_domains:
            continue
        seen_domains.add(domain)

        log.info("Mining contacts: %s", domain)
        start_url = f"https://{domain}/"
        visited: Set[str] = set()
        q: deque = deque([(start_url, 0)])
        found_person_emails: List[str] = []

        while q and len(visited) < max_pages:
            url, depth = q.popleft()
            if depth > max_depth or url in visited:
                continue
            visited.add(url)

            # Skip excluded file extensions
            parsed_path = urlparse(url).path.lower()
            if any(parsed_path.endswith(ext) for ext in exclude_exts):
                continue

            try:
                rate_limit_sleep(rate_s)
                r = requests.get(url, timeout=timeout, headers={"User-Agent": ua}, allow_redirects=True)
                if r.status_code >= 400:
                    continue
                html = r.text
            except Exception as exc:
                log.debug("Crawl error %s: %s", url, exc)
                continue

            emails = extract_emails(html)
            phones = extract_phones(html)

            for e in emails:
                local = e.split("@")[0]
                is_role = local in ROLE_INBOX_PREFIXES or local.rstrip("s") in ROLE_INBOX_PREFIXES
                is_person = bool(PERSON_EMAIL_RE.match(e))

                contacts_rows.append({
                    "source_domain": domain,
                    "source_url": url,
                    "email": e,
                    "email_type": "role_inbox" if is_role else ("person" if is_person else "other"),
                    "phone": phones[0] if phones else "",
                    "person_name": "",
                    "person_role": "",
                    "discovered_at_utc": utc_now_iso(),
                })

                if is_person:
                    found_person_emails.append(e)

            soup = BeautifulSoup(html, "lxml")
            for a in soup.select("a[href]"):
                href = a.get("href", "")
                if not href:
                    continue
                next_url = urljoin(url, href)
                parsed = urlparse(next_url)
                if parsed.scheme not in ("http", "https"):
                    continue
                if parsed.netloc and domain not in parsed.netloc:
                    continue
                path = (parsed.path or "").lower()
                if any(k in path for k in include_keywords) or depth == 0:
                    q.append((next_url, depth + 1))

        # Infer pattern from person emails found
        pattern = _infer_pattern(found_person_emails)
        if pattern:
            patterns_rows.append({
                "source_domain": domain,
                "pattern": pattern,
                "pattern_basis_emails": ",".join(sorted(set(found_person_emails))[:5]),
                "example_names": _extract_name_examples(found_person_emails),
            })
            log.info("  Pattern for %s: %s", domain, pattern)
        else:
            log.info("  No person email pattern found for %s", domain)

    contacts_df = pd.DataFrame(contacts_rows) if contacts_rows else pd.DataFrame(
        columns=["source_domain", "source_url", "email", "email_type", "phone",
                 "person_name", "person_role", "discovered_at_utc"]
    )
    patterns_df = pd.DataFrame(patterns_rows) if patterns_rows else pd.DataFrame(
        columns=["source_domain", "pattern", "pattern_basis_emails", "example_names"]
    )

    log.info("Contact mining complete: %d emails found across %d domains",
             len(contacts_df), len(seen_domains))
    return contacts_df, patterns_df


def _infer_pattern(person_emails: List[str]) -> str:
    if not person_emails:
        return ""
    patterns = []
    for e in person_emails:
        local = e.split("@")[0]
        if re.match(r"^[a-z]+\.[a-z]+$", local):
            patterns.append("first.last")
        elif re.match(r"^[a-z]+_[a-z]+$", local):
            patterns.append("first_last")
        elif re.match(r"^[a-z][a-z]{2,}$", local) and len(local) >= 4:
            patterns.append("flast")
        elif re.match(r"^[a-z]\.[a-z]+$", local):
            patterns.append("f.last")
    if not patterns:
        return ""
    # Return most common pattern
    return max(set(patterns), key=patterns.count)


def _extract_name_examples(person_emails: List[str]) -> str:
    """Extract first/last name hints from email locals for candidate generation."""
    names = []
    for e in person_emails[:3]:
        local = e.split("@")[0]
        if "." in local:
            parts = local.split(".")
            if len(parts) == 2:
                names.append(f"{parts[0].title()} {parts[1].title()}")
        elif "_" in local:
            parts = local.split("_")
            if len(parts) == 2:
                names.append(f"{parts[0].title()} {parts[1].title()}")
    return "; ".join(names)
