from __future__ import annotations

import logging
import re
from typing import Dict, List, Set

import pandas as pd
import requests
from bs4 import BeautifulSoup

from .utils import normalize_text

log = logging.getLogger("cranegenius.contact_page_finder")

CONTACT_PATHS = ["/contact", "/about", "/team", "/staff", "/company", "/leadership"]
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
NAME_RE = re.compile(r"\b([A-Z][a-z]{1,20})\s+([A-Z][a-z]{1,24})\b")
NAV_NAME_BLOCKLIST = {
    "about",
    "contact",
    "locations",
    "resources",
    "services",
    "blog",
    "team",
    "support",
    "careers",
    "privacy",
    "terms",
    "new",
    "roof",
    "request",
    "quote",
    "learn",
    "more",
    "free",
    "estimate",
    "call",
    "today",
    "us",
    "living",
}
CTA_NAME_BLOCK_PHRASES = {
    "new roof",
    "request quote",
    "learn more",
    "free estimate",
    "call today",
    "contact us",
}
MARKETING_NAME_BLOCK_PHRASES = {
    "years experience",
    "featured an",
    "article living",
    "magazine adrian",
    "comments the",
}
CONTENT_CONTEXT_BLOCK_HINTS = ("blog", "article", "post", "comment", "comments", "magazine", "news", "content")


def _is_content_context(source_url: str, text: str = "") -> bool:
    src = normalize_text(source_url).lower()
    low = normalize_text(text).lower()
    return any(h in src or h in low for h in CONTENT_CONTEXT_BLOCK_HINTS)


def _is_likely_person_name(first_name: str, last_name: str) -> bool:
    first = normalize_text(first_name).strip()
    last = normalize_text(last_name).strip()
    if not first or not last:
        return False
    first_low = first.lower().strip(".")
    last_low = last.lower().strip(".")
    if first_low == last_low:
        return False
    if first_low in NAV_NAME_BLOCKLIST or last_low in NAV_NAME_BLOCKLIST:
        return False
    phrase = f"{first_low} {last_low}"
    if phrase in CTA_NAME_BLOCK_PHRASES or phrase in MARKETING_NAME_BLOCK_PHRASES:
        return False
    return True


def _fetch(url: str) -> str:
    try:
        resp = requests.get(
            url,
            timeout=6,
            allow_redirects=True,
            headers={"User-Agent": "CraneGeniusContactFinder/1.0"},
        )
        return resp.text if resp.ok else ""
    except Exception:
        return ""


def _extract_candidates(html: str, domain: str, source_url: str) -> List[Dict[str, object]]:
    soup = BeautifulSoup(html or "", "lxml")
    text = soup.get_text(" ", strip=True)
    if _is_content_context(source_url, text):
        return []
    rows: List[Dict[str, object]] = []
    seen: Set[str] = set()

    for a in soup.select('a[href^="mailto:"]'):
        href = normalize_text(a.get("href", ""))
        email = normalize_text(href.replace("mailto:", "")).split("?")[0].strip().lower()
        if not email or not email.endswith(domain):
            continue
        if email in seen:
            continue
        seen.add(email)

        anchor_text = normalize_text(a.get_text(" ", strip=True))
        name_match = NAME_RE.search(anchor_text)
        first_name = name_match.group(1) if name_match else ""
        last_name = name_match.group(2) if name_match else ""
        if not _is_likely_person_name(first_name, last_name):
            first_name = ""
            last_name = ""
        full_name = f"{first_name} {last_name}".strip()

        rows.append(
            {
                "first_name": first_name,
                "last_name": last_name,
                "full_name": full_name,
                "source_url": source_url,
                "found_email": email,
            }
        )

    for m in EMAIL_RE.finditer(text):
        email = normalize_text(m.group(0)).lower()
        if not email.endswith(domain):
            continue
        if email in seen:
            continue
        seen.add(email)

        left = max(0, m.start() - 120)
        context = text[left : m.start()]
        name_matches = NAME_RE.findall(context)
        first_name, last_name = (name_matches[-1] if name_matches else ("", ""))
        if not _is_likely_person_name(first_name, last_name):
            first_name = ""
            last_name = ""
        full_name = f"{first_name} {last_name}".strip()

        rows.append(
            {
                "first_name": first_name,
                "last_name": last_name,
                "full_name": full_name,
                "source_url": source_url,
                "found_email": email,
            }
        )

    return rows


def discover_contact_people(company_domains_df: pd.DataFrame) -> pd.DataFrame:
    """Discover contact/team page names/emails for valid domains."""
    columns = [
        "first_name",
        "last_name",
        "full_name",
        "contractor_name_normalized",
        "contractor_domain",
        "title",
        "title_confirmed",
        "source_url",
        "is_role_inbox",
        "found_email",
    ]
    if company_domains_df.empty:
        return pd.DataFrame(columns=columns)

    rows: List[Dict[str, object]] = []
    for _, row in company_domains_df.iterrows():
        domain = normalize_text(row.get("contractor_domain", "")).lower().strip()
        if not domain:
            continue
        if not bool(row.get("domain_valid", False)):
            continue

        company = normalize_text(row.get("contractor_name_normalized", "")).lower().strip()
        for path in CONTACT_PATHS:
            url = f"https://{domain}{path}"
            html = _fetch(url)
            if not html:
                continue
            extracted = _extract_candidates(html, domain, url)
            if extracted:
                log.info(
                    "Contact page scan | company=%s domain=%s path=%s hits=%d",
                    company,
                    domain,
                    path,
                    len(extracted),
                )
            for hit in extracted:
                full_name = normalize_text(hit.get("full_name", ""))
                first_name = normalize_text(hit.get("first_name", ""))
                last_name = normalize_text(hit.get("last_name", ""))
                if not full_name and first_name and last_name:
                    full_name = f"{first_name} {last_name}".strip()

                rows.append(
                    {
                        "first_name": first_name,
                        "last_name": last_name,
                        "full_name": full_name,
                        "contractor_name_normalized": company,
                        "contractor_domain": domain,
                        "title": "",
                        "title_confirmed": False,
                        "source_url": normalize_text(hit.get("source_url", "")),
                        "is_role_inbox": False,
                        "found_email": normalize_text(hit.get("found_email", "")).lower(),
                    }
                )

    out = pd.DataFrame(rows, columns=columns)
    if out.empty:
        return out
    # keep only rows with usable names for permutation generation
    out = out[(out["first_name"].astype(str).str.strip() != "") & (out["last_name"].astype(str).str.strip() != "")]
    out = out.drop_duplicates(subset=["contractor_name_normalized", "contractor_domain", "first_name", "last_name"])
    return out.reset_index(drop=True)
