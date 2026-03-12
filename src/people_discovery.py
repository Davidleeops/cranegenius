from __future__ import annotations

import logging
import re
from collections import deque
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import quote_plus, urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag

from .utils import normalize_text

log = logging.getLogger("cranegenius.people_discovery")

DISCOVERY_PATHS = ["/", "/about", "/about-us", "/team", "/staff", "/leadership", "/people", "/management"]
NAV_CLASS_HINTS = ["menu", "nav", "footer", "sidebar", "breadcrumb", "cookie", "banner", "social"]
NAV_LINK_HINTS = ["#", "javascript:", "/privacy", "/terms", "/cookies", "/sitemap", "/careers", "/blog"]
PROFILE_PATH_HINT_RE = re.compile(r"(team|about|leadership|staff|people|management|contact)", re.IGNORECASE)

TITLE_KEYWORDS = [
    ("Project Manager", "project manager"),
    ("Estimator", "estimator"),
    ("Preconstruction Manager", "preconstruction"),
    ("Superintendent", "superintendent"),
    ("Director of Construction", "director construction"),
    ("Operations Manager", "operations manager"),
]

TITLE_NORMALIZATION_MAP = {
    # Project Manager
    "project manager": "Project Manager",
    "pm": "Project Manager",
    "sr. project manager": "Project Manager",
    "senior pm": "Project Manager",
    "senior project manager": "Project Manager",
    "pm ii": "Project Manager",
    "pm iii": "Project Manager",
    "project manager ii": "Project Manager",
    "project manager iii": "Project Manager",
    "lead project manager": "Project Manager",
    "associate pm": "Project Manager",
    "assistant project manager": "Project Manager",
    "junior pm": "Project Manager",
    "project manager i": "Project Manager",
    "proj mgr": "Project Manager",
    "proj manager": "Project Manager",
    "project mgr": "Project Manager",
    "sr pm": "Project Manager",
    # Estimator
    "estimator": "Estimator",
    "sr. estimator": "Estimator",
    "senior estimator": "Estimator",
    "lead estimator": "Estimator",
    "chief estimator": "Estimator",
    "estimator ii": "Estimator",
    "estimator iii": "Estimator",
    "estimating manager": "Estimator",
    "director of estimating": "Estimator",
    "preconstruction estimator": "Estimator",
    "project estimator": "Estimator",
    "construction estimator": "Estimator",
    "jr. estimator": "Estimator",
    "junior estimator": "Estimator",
    "bid manager": "Estimator",
    "bidding manager": "Estimator",
    # Preconstruction Manager
    "preconstruction manager": "Preconstruction Manager",
    "precon manager": "Preconstruction Manager",
    "pre-construction manager": "Preconstruction Manager",
    "vp preconstruction": "Preconstruction Manager",
    "director of preconstruction": "Preconstruction Manager",
    "director precon": "Preconstruction Manager",
    "head of preconstruction": "Preconstruction Manager",
    "preconstruction director": "Preconstruction Manager",
    "precon lead": "Preconstruction Manager",
    "pre-con manager": "Preconstruction Manager",
    "vp of precon": "Preconstruction Manager",
    # Superintendent
    "superintendent": "Superintendent",
    "supt": "Superintendent",
    "super": "Superintendent",
    "general superintendent": "Superintendent",
    "sr. superintendent": "Superintendent",
    "senior superintendent": "Superintendent",
    "field superintendent": "Superintendent",
    "project superintendent": "Superintendent",
    "assistant superintendent": "Superintendent",
    "asst superintendent": "Superintendent",
    "site superintendent": "Superintendent",
    "lead superintendent": "Superintendent",
    # Director of Construction
    "director of construction": "Director of Construction",
    "construction director": "Director of Construction",
    "director construction": "Director of Construction",
    "vp construction": "Director of Construction",
    "vp of construction": "Director of Construction",
    "vice president construction": "Director of Construction",
    "director of field operations": "Director of Construction",
    "regional director construction": "Director of Construction",
    "construction manager": "Director of Construction",
    "sr. construction manager": "Director of Construction",
    # Operations Manager
    "operations manager": "Operations Manager",
    "director of operations": "Operations Manager",
    "vp operations": "Operations Manager",
    "vp of operations": "Operations Manager",
    "operations director": "Operations Manager",
    "field operations manager": "Operations Manager",
    "regional operations manager": "Operations Manager",
    "sr. operations manager": "Operations Manager",
    "construction operations manager": "Operations Manager",
}

SUFFIXES = {"JR", "SR", "II", "III"}
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
PERSON_CONTEXT_HINTS = (
    "project manager",
    "estimator",
    "superintendent",
    "director",
    "manager",
    "operations",
    "president",
    "engineer",
    "leadership",
)
NAME_RE = re.compile(r"\b([A-Za-z]{2,20})\s+([A-Za-z]\.?|[A-Za-z]{2,20})\s+([A-Za-z]{2,20})(?:\s+(Jr|Sr|II|III))?\b|\b([A-Za-z]{2,20})\s+([A-Za-z]{2,20})(?:\s+(Jr|Sr|II|III))?\b")


def normalize_title(raw_title: str) -> str:
    """Map a raw title string to a canonical title bucket when possible."""
    clean = normalize_text(raw_title).lower().strip()
    return TITLE_NORMALIZATION_MAP.get(clean, raw_title)


def _element_is_noise(el: Tag) -> bool:
    """Return True when element should be ignored for people extraction."""
    if el.name in {"nav", "header", "footer"}:
        return True
    cls = " ".join(el.get("class", [])).lower()
    if any(h in cls for h in NAV_CLASS_HINTS):
        return True
    if el.name == "a":
        href = normalize_text(el.get("href", "")).lower()
        text = normalize_text(el.get_text(" ", strip=True)).lower()
        if any(h in href for h in NAV_LINK_HINTS):
            return True
        if len(text.split()) <= 3 and any(x in text for x in ["home", "about", "services", "contact", "blog"]):
            return True
    return False


def _token_valid(token: str) -> bool:
    """Validate a candidate name token."""
    if not (2 <= len(token) <= 20):
        return False
    if any(ch.isdigit() for ch in token):
        return False
    if not token[0].isalpha():
        return False
    return True


def _is_nav_name_token(token: str) -> bool:
    clean = normalize_text(token).lower().strip(".")
    return clean in NAV_NAME_BLOCKLIST


def _is_company_echo_name(first: str, last: str, company_name: str) -> bool:
    company = normalize_text(company_name).lower()
    if not company:
        return False
    phrase = f"{first.lower()} {last.lower()}"
    return bool(re.search(rf"\b{re.escape(phrase)}\b", company))


def _is_likely_person_name(first: str, last: str) -> bool:
    if not (_token_valid(first) and _token_valid(last)):
        return False
    if first.lower() == last.lower():
        return False
    if _is_nav_name_token(first) or _is_nav_name_token(last):
        return False
    phrase = f"{first.lower()} {last.lower()}"
    if phrase in CTA_NAME_BLOCK_PHRASES or phrase in MARKETING_NAME_BLOCK_PHRASES:
        return False
    return True


def _normalize_person_tokens(tokens: List[str]) -> Tuple[Optional[str], Optional[str], bool]:
    """Normalize name tokens and classify all-caps source."""
    if len(tokens) not in {2, 3}:
        return None, None, False
    stripped = [t.replace(".", "").strip() for t in tokens if t.strip()]
    if len(stripped) not in {2, 3}:
        return None, None, False

    is_all_caps = all(t.isupper() for t in stripped)
    if is_all_caps:
        stripped = [t.title() for t in stripped]

    if len(stripped) == 2:
        first, last = stripped
        if _is_likely_person_name(first, last):
            return first, last, is_all_caps
        return None, None, False

    first, mid, last = stripped
    mid_clean = mid.upper()
    mid_ok = (len(mid_clean) == 1) or (mid_clean in SUFFIXES)
    if not mid_ok and len(mid_clean) > 2:
        return None, None, False
    if _is_likely_person_name(first, last):
        return first, last, is_all_caps
    return None, None, False


def _title_from_text(text: str) -> Optional[str]:
    """Extract canonical title from free text using title keyword map."""
    low = normalize_text(text).lower()
    for canonical, key in TITLE_KEYWORDS:
        if key in low:
            return normalize_title(canonical)
    for raw, canonical in TITLE_NORMALIZATION_MAP.items():
        if raw in low:
            return canonical
    return None


def _has_person_context(text: str) -> bool:
    low = normalize_text(text).lower()
    return any(h in low for h in PERSON_CONTEXT_HINTS)


def _is_content_context(source_url: str, text: str) -> bool:
    src = normalize_text(source_url).lower()
    low = normalize_text(text).lower()
    return any(h in src or h in low for h in CONTENT_CONTEXT_BLOCK_HINTS)


def _secondary_title_probe(el: Tag) -> Optional[str]:
    """Probe sibling/parent context up to 2 levels when local text lacks title."""
    cur: Optional[Tag] = el
    for _ in range(2):
        if cur is None:
            break
        siblings_text = " ".join(normalize_text(s.get_text(" ", strip=True)) for s in list(cur.previous_siblings)[:2] + list(cur.next_siblings)[:2] if isinstance(s, Tag))
        t = _title_from_text(siblings_text)
        if t:
            return t
        cur = cur.parent if isinstance(cur.parent, Tag) else None
        if cur:
            t = _title_from_text(normalize_text(cur.get_text(" ", strip=True)))
            if t:
                return t
    return None


def _fetch_url(url: str) -> Tuple[str, str]:
    """Fetch URL with redirect support and return (html, final_url)."""
    try:
        resp = requests.get(
            url,
            timeout=8,
            allow_redirects=True,
            headers={"User-Agent": "CraneGeniusPeopleBot/1.0"},
        )
        if not resp.ok:
            return "", ""
        return resp.text, resp.url
    except Exception:
        return "", ""


def _crawl_domain(domain: str, max_depth: int = 2, max_pages: int = 20) -> List[Tuple[str, str]]:
    """Crawl initial profile paths and related in-domain profile links up to depth 2."""
    start_urls = [f"https://{domain}{p}" for p in DISCOVERY_PATHS]
    queue = deque([(u, 0) for u in start_urls])
    seen: Set[str] = set()
    pages: List[Tuple[str, str]] = []

    while queue and len(pages) < max_pages:
        url, depth = queue.popleft()
        if url in seen or depth > max_depth:
            continue
        seen.add(url)

        html, final_url = _fetch_url(url)
        if not html:
            continue
        pages.append((final_url or url, html))
        if depth >= max_depth:
            continue

        soup = BeautifulSoup(html, "lxml")
        for a in soup.select("a[href]"):
            if _element_is_noise(a):
                continue
            href = normalize_text(a.get("href", ""))
            if not href:
                continue
            joined = urljoin(final_url or url, href)
            parsed = urlparse(joined)
            if parsed.scheme not in {"http", "https"}:
                continue
            if domain not in parsed.netloc.lower():
                continue
            if not PROFILE_PATH_HINT_RE.search(parsed.path or ""):
                continue
            if joined not in seen:
                queue.append((joined, depth + 1))

    return pages


def _extract_from_page(
    html: str,
    source_url: str,
    company: str,
    domain: str,
    city: str,
    state: str,
) -> List[Dict[str, object]]:
    """Extract people from one HTML page with title proximity and fallback unconfirmed titles."""
    rows: List[Dict[str, object]] = []
    soup = BeautifulSoup(html, "lxml")
    seen_persons: Set[Tuple[str, str]] = set()

    for el in soup.find_all(["p", "li", "div", "span", "h1", "h2", "h3", "h4", "h5", "h6", "a"]):
        if _element_is_noise(el):
            continue
        text = normalize_text(el.get_text(" ", strip=True))
        if not text:
            continue
        if text.isupper() and len(text.split()) > 4:
            continue

        for match in NAME_RE.finditer(text):
            if match.group(1) and match.group(3):
                tokens = [match.group(1), match.group(2), match.group(3)]
                if match.group(4):
                    tokens.append(match.group(4))
            else:
                tokens = [match.group(5), match.group(6)]
                if match.group(7):
                    tokens.append(match.group(7))
            tokens = [t for t in tokens if t]

            first_name, last_name, allcaps = _normalize_person_tokens(tokens)
            if not first_name or not last_name:
                continue
            if _is_company_echo_name(first_name, last_name, company):
                continue
            if (first_name.lower(), last_name.lower()) in seen_persons:
                continue

            span = match.span()
            left = max(0, span[0] - 200)
            right = min(len(text), span[1] + 200)
            local_window = text[left:right]
            title = _title_from_text(local_window)
            title_confirmed = True
            if not title:
                title = _secondary_title_probe(el)
            if not title:
                title = "unconfirmed"
                title_confirmed = False

            if _is_content_context(source_url, local_window):
                continue

            if (not title_confirmed) and (not _has_person_context(local_window)):
                continue

            source = "website_allcaps" if allcaps else "website"
            rows.append(
                {
                    "contractor_name_normalized": company,
                    "contractor_domain": domain,
                    "first_name": first_name,
                    "last_name": last_name,
                    "title": title,
                    "title_confirmed": title_confirmed,
                    "discovery_source": source,
                    "role_inbox_tier": None,
                    "project_city": city,
                    "project_state": state,
                    "company_name": company,
                    "domain": domain,
                    "source": source,
                    "source_url": source_url,
                    "verification_status": "",
                    "is_role_inbox": False,
                }
            )
            seen_persons.add((first_name.lower(), last_name.lower()))

    return rows


def _linkedin_fallback(company: str, domain: str, city: str, state: str, limit: int) -> List[Dict[str, object]]:
    """Attempt lightweight LinkedIn name discovery via public search result titles."""
    rows: List[Dict[str, object]] = []
    query = f'site:linkedin.com/in "{company}" "project manager"'
    url = f"https://html.duckduckgo.com/html/?q={quote_plus(query)}"
    html, final_url = _fetch_url(url)
    if not html:
        return rows
    soup = BeautifulSoup(html, "lxml")
    for a in soup.select("a.result__a, a[href*='linkedin.com/in/']"):
        text = normalize_text(a.get_text(" ", strip=True))
        href = normalize_text(a.get("href", ""))
        m = NAME_RE.search(text)
        if not m:
            continue
        tokens = []
        if m.group(1) and m.group(3):
            tokens = [m.group(1), m.group(2), m.group(3)]
        elif m.group(5) and m.group(6):
            tokens = [m.group(5), m.group(6)]
        tokens = [t for t in tokens if t]
        first_name, last_name, allcaps = _normalize_person_tokens(tokens)
        if not first_name or not last_name:
            continue
        source = "website_allcaps" if allcaps else "linkedin_fallback"
        rows.append(
            {
                "contractor_name_normalized": company,
                "contractor_domain": domain,
                "first_name": first_name,
                "last_name": last_name,
                "title": "Project Manager",
                "title_confirmed": True,
                "discovery_source": source,
                "role_inbox_tier": None,
                "project_city": city,
                "project_state": state,
                "company_name": company,
                "domain": domain,
                "source": "linkedin_fallback",
                "source_url": href or final_url,
                "verification_status": "",
                "is_role_inbox": False,
            }
        )
        if len(rows) >= limit:
            break
    return rows


def generate_role_inbox_fallback(domain: str) -> List[Dict[str, object]]:
    """Return prioritized role inbox fallback candidates for validated domains."""
    clean = normalize_text(domain).lower()
    if not clean:
        return []
    prefixes = ["estimating", "bids", "projects", "info"]
    out: List[Dict[str, object]] = []
    for i, prefix in enumerate(prefixes, start=1):
        out.append(
            {
                "email": f"{prefix}@{clean}",
                "role_inbox_tier": i,
                "domain": clean,
                "discovery_source": "role_inbox_fallback",
            }
        )
    return out


def discover_people(company_domains_df: pd.DataFrame, max_people_per_company: int = 3) -> pd.DataFrame:
    """Discover named people and fallback role inbox records for validated company domains."""
    columns = [
        "contractor_name_normalized",
        "contractor_domain",
        "first_name",
        "last_name",
        "title",
        "title_confirmed",
        "discovery_source",
        "role_inbox_tier",
        "project_city",
        "project_state",
        "company_name",
        "domain",
        "source",
        "source_url",
        "verification_status",
        "is_role_inbox",
    ]
    if company_domains_df.empty:
        return pd.DataFrame(columns=columns)

    all_rows: List[Dict[str, object]] = []
    for _, row in company_domains_df.iterrows():
        company = normalize_text(row.get("contractor_name_normalized", "")).lower()
        domain = normalize_text(row.get("contractor_domain", "")).lower()
        city = normalize_text(row.get("project_city", ""))
        state = normalize_text(row.get("project_state", ""))
        domain_valid = bool(row.get("domain_valid", False))
        if not company or not domain:
            continue

        found: List[Dict[str, object]] = []
        pages = _crawl_domain(domain, max_depth=2, max_pages=20)
        for source_url, html in pages:
            found.extend(_extract_from_page(html, source_url, company, domain, city, state))
            if len(found) >= max_people_per_company:
                break

        if not found:
            found.extend(_linkedin_fallback(company, domain, city, state, max_people_per_company))

        if found:
            df = pd.DataFrame(found).drop_duplicates(
                subset=["contractor_name_normalized", "first_name", "last_name", "title"]
            )
            all_rows.extend(df.head(max_people_per_company).to_dict(orient="records"))
        elif domain_valid:
            for fallback in generate_role_inbox_fallback(domain):
                all_rows.append(
                    {
                        "contractor_name_normalized": company,
                        "contractor_domain": domain,
                        "first_name": "",
                        "last_name": "",
                        "title": "unconfirmed",
                        "title_confirmed": False,
                        "discovery_source": fallback["discovery_source"],
                        "role_inbox_tier": fallback["role_inbox_tier"],
                        "project_city": city,
                        "project_state": state,
                        "company_name": company,
                        "domain": domain,
                        "source": "role_inbox_fallback",
                        "source_url": f"https://{domain}",
                        "verification_status": "",
                        "is_role_inbox": True,
                    }
                )

    out = pd.DataFrame(all_rows, columns=columns)
    people_found_count = int(len(out[~out["is_role_inbox"]])) if not out.empty else 0
    companies_with_people = int(out[~out["is_role_inbox"]]["contractor_name_normalized"].nunique()) if not out.empty else 0
    avg_people_per_company = (people_found_count / companies_with_people) if companies_with_people else 0.0
    log.info(
        "People discovery stats: people_found_count=%d companies_with_people=%d avg_people_per_company=%.3f",
        people_found_count,
        companies_with_people,
        avg_people_per_company,
    )
    return out
