from __future__ import annotations

import logging
import re
import subprocess
from difflib import SequenceMatcher
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import parse_qs, quote_plus, unquote, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup

from .utils import normalize_text

log = logging.getLogger("cranegenius.domain_discovery")

DATA_DIR = Path("data")
SEED_PATH = DATA_DIR / "company_domain_seed.csv"
ENRICHED_PATH = DATA_DIR / "enriched_companies.csv"
CI_SEED_LOCAL_CSV_PATH = DATA_DIR / "imported_contact_sources" / "company_domain_seed_enriched.csv"
CI_SEED_EXPORT_XLSX_PATH = Path.home() / "data_runtime" / "exports" / "company_domain_seed_candidates.xlsx"

PARKING_PHRASES_STRONG = [
    "domain for sale",
    "buy this domain",
    "this domain is parked",
    "godaddy parking",
    "hugedomains",
    "sedo domain parking",
]
PARKING_PHRASES_WEAK = [
    "sedo",
    "godaddy",
    "parked",
    "for sale",
    "hugedomains",
]
CONSTRUCTION_KEYWORDS = ["construction", "contractor", "crane", "building", "industrial", "mechanical"]
VALID_STATUS_CODES = {200, 301, 302}
REJECT_STATUS_CODES = {403, 404, 410}
DISPOSABLE_DOMAINS = {
    "mailinator.com",
    "temp-mail.org",
    "tempmail.com",
    "guerrillamail.com",
    "10minutemail.com",
    "yopmail.com",
    "dispostable.com",
    "trashmail.com",
}
SEARCH_EXCLUDED_DOMAINS = {
    "linkedin.com",
    "facebook.com",
    "instagram.com",
    "yelp.com",
    "mapquest.com",
    "yellowpages.com",
    "buzzfile.com",
    "manta.com",
    "chamberofcommerce.com",
}
SEARCH_BASE_URL = "https://duckduckgo.com/html/"
SEARCH_FALLBACK_QUERY_BUDGET = 3
SEARCH_FALLBACK_CANDIDATE_BUDGET = 10
MAX_SEARCH_FALLBACK_ATTEMPTS = 25
SEARCH_TOKEN_STOPWORDS = {"inc", "llc", "corp", "co", "group", "services", "service", "solutions", "company", "the", "and"}
COMPANY_GENERIC_TOKENS = {"inc", "llc", "corp", "co", "company", "group", "services", "service", "solutions", "construction", "contracting", "contractor", "builders", "building", "the", "and", "of"}
AMBIGUOUS_GENERIC_ROOTS = {"aa", "priority", "expert", "tempo"}
_SEARCH_RESOLVE_CACHE: Dict[str, Dict[str, object]] = {}

PRIMARY_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)
FALLBACK_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) "
    "Gecko/20100101 Firefox/124.0"
)

SLUG_STOPWORDS_RE = re.compile(
    r"\b(inc|llc|corp|co|ltd|construction|contracting|contractors|group|services|solutions)\b",
    re.IGNORECASE,
)
NON_WORD_RE = re.compile(r"[^a-z0-9\s-]+")
PHONE_RE = re.compile(r"\(?\b\d{3}\)?[\s\-.]?\d{3}[\s\-.]?\d{4}\b|\b\d{10}\b")
ADDRESS_START_RE = re.compile(
    r"\b\d{1,6}\s+[a-z0-9.\-]+(?:\s+[a-z0-9.\-]+){0,6}\s+"
    r"(?:st|street|rd|road|ave|avenue|blvd|boulevard|ln|lane|dr|drive|pkwy|parkway|hwy|highway|ct|court|cir|circle|way|pl|place|trl|trail)\b",
    re.IGNORECASE,
)
CITY_STATE_ZIP_TAIL_RE = re.compile(r",?\s*[a-z .'-]+,\s*[a-z]{2}\s+\d{5}(?:-\d{4})?\s*$", re.IGNORECASE)
SUITE_FRAGMENT_RE = re.compile(r"(?:,?\s*(?:(?:ste|suite|apt|unit)\b|#)\s*[\w-]+)\b", re.IGNORECASE)
PO_BOX_TAIL_RE = re.compile(r"\b(?:p\.?\s*o\.?\s*box)\b.*$", re.IGNORECASE)


def normalize_company_slug(company_name: str) -> str:
    """Normalize a company name into a lookup slug for domain variant generation."""
    value = normalize_text(company_name)
    value = value.replace("&", " and ")
    value = value.lower()
    value = SLUG_STOPWORDS_RE.sub(" ", value)
    value = NON_WORD_RE.sub(" ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value.replace(" ", "")


def _normalize_full_name_hyphenated(company_name: str) -> str:
    """Build a hyphenated normalized full name variant for fallback domain guesses."""
    value = normalize_text(company_name).replace("&", " and ").lower()
    value = NON_WORD_RE.sub(" ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value.replace(" ", "-")


def clean_company_name(company_name: str) -> str:
    """Clean noisy company text before domain slug/discovery generation."""
    raw = normalize_text(company_name)
    if not raw:
        return ""
    text = raw.lower()

    # Strip explicit phone numbers and parenthetical phone tails.
    text = PHONE_RE.sub(" ", text)
    text = re.sub(r"[\\/]+\s*(?:\d|po\s*box|p\.?o\.?\s*box|[a-z]{2}\b|contact|locations?)\b.*$", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\(\s*\)", " ", text)
    text = re.sub(r"\(([^)]*)\)\s*$", lambda m: " " if len(re.sub(r"\D", "", m.group(1))) >= 7 else m.group(0), text)

    # Remove suite/unit fragments.
    text = PO_BOX_TAIL_RE.sub(" ", text)
    text = SUITE_FRAGMENT_RE.sub(" ", text)

    # Cut obvious address tails when a street address starts.
    address_match = ADDRESS_START_RE.search(text)
    if address_match:
        text = text[: address_match.start()]

    # Remove trailing city/state/zip style tails.
    text = CITY_STATE_ZIP_TAIL_RE.sub(" ", text)

    # Remove trailing contact/location suffix fragments.
    text = re.sub(r"(?:,?\s*(?:contact|locations?|resources?|support|careers|privacy|terms))+$", " ", text, flags=re.IGNORECASE)

    # Trim punctuation noise and collapse spaces.
    text = re.sub(r"[,.\-–—:;]+$", "", text)
    text = re.sub(r"[\\/]+$", "", text)
    text = re.sub(r"\s+", " ", text).strip(" ,.-")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def generate_domain_variants(slug: str, full_name: str, state_abbr: str = "") -> List[str]:
    """Return ordered domain variant candidates for a company."""
    slug_clean = re.sub(r"[^a-z0-9]", "", normalize_text(slug).lower())
    full_name_hyphenated = _normalize_full_name_hyphenated(full_name)
    variants: List[str] = []
    if not slug_clean:
        return variants

    variants.extend(
        [
            f"{slug_clean}.com",
            f"{slug_clean}construction.com",
            f"{slug_clean}contracting.com",
            f"{slug_clean}group.com",
            f"{slug_clean}inc.com",
            f"{slug_clean}co.com",
        ]
    )
    if full_name_hyphenated:
        variants.append(f"{full_name_hyphenated}.com")
    state = re.sub(r"[^a-z]", "", normalize_text(state_abbr).lower())
    if state:
        variants.append(f"{slug_clean}{state}.com")
    return variants


def _check_parking_and_keywords(url: str) -> Dict[str, object]:
    """Fetch page text and inspect parking signals + construction keyword relevance."""
    try:
        resp = requests.get(
            url,
            timeout=5,
            headers={"User-Agent": PRIMARY_UA},
            allow_redirects=True,
        )
        soup = BeautifulSoup(resp.text, "lxml")
        page_text = normalize_text(soup.get_text(" ", strip=True)).lower()
        title = normalize_text((soup.title.get_text(" ", strip=True) if soup.title else "")).lower()
        meta_desc_tag = soup.find("meta", attrs={"name": re.compile("^description$", re.IGNORECASE)})
        meta_desc = normalize_text(meta_desc_tag.get("content", "") if meta_desc_tag else "").lower()
        parked_eval = _detect_parked_domain(page_text=page_text, title=title, meta_desc=meta_desc)
        if parked_eval.get("parked"):
            return {
                "parking": True,
                "parked_evidence": parked_eval.get("parked_evidence"),
                "construction_keyword_match": False,
            }
        combined = f"{title} {meta_desc}"
        keyword_match = any(k in combined for k in CONSTRUCTION_KEYWORDS)
        return {
            "parking": False,
            "parked_evidence": None,
            "construction_keyword_match": keyword_match,
        }
    except requests.exceptions.SSLError:
        return {"parking": False, "parked_evidence": None, "construction_keyword_match": False, "error": "ssl_error"}
    except requests.exceptions.Timeout:
        return {"parking": False, "parked_evidence": None, "construction_keyword_match": False, "error": "timeout"}
    except Exception:
        return {"parking": False, "parked_evidence": None, "construction_keyword_match": False, "error": "request_error"}


def _find_phrase_matches(text: str, phrases: List[str]) -> List[str]:
    """Return normalized phrases matched as whole phrases in normalized text."""
    matches: List[str] = []
    for phrase in phrases:
        parts = [p for p in phrase.split() if p]
        if not parts:
            continue
        pattern = r"\b" + r"\s+".join(re.escape(part) for part in parts) + r"\b"
        if re.search(pattern, text):
            matches.append(phrase)
    return matches


def _detect_parked_domain(*, page_text: str, title: str, meta_desc: str) -> Dict[str, object]:
    """
    Detect parked domains with stronger evidence:
    - any strong explicit parking phrase, or
    - multiple weak parking signals.
    """
    combined = normalize_text(f"{title} {meta_desc} {page_text}").lower()
    strong_matches = _find_phrase_matches(combined, PARKING_PHRASES_STRONG)
    if strong_matches:
        return {"parked": True, "parked_evidence": strong_matches[0]}

    weak_matches = _find_phrase_matches(combined, PARKING_PHRASES_WEAK)
    weak_unique = sorted(set(weak_matches))
    if len(weak_unique) >= 2:
        return {"parked": True, "parked_evidence": ", ".join(weak_unique)}

    return {"parked": False, "parked_evidence": None}


def _is_disposable_domain(domain: str) -> bool:
    """Return True when the candidate domain is in a disposable provider family."""
    clean_domain = normalize_text(domain).lower().strip(".")
    if not clean_domain:
        return False
    return any(clean_domain == bad or clean_domain.endswith(f".{bad}") for bad in DISPOSABLE_DOMAINS)


def _mx_lookup_with_nslookup(domain: str) -> Tuple[bool, Optional[str]]:
    """Fallback MX validation using nslookup when dnspython is unavailable."""
    try:
        proc = subprocess.run(
            ["nslookup", "-type=mx", domain],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return False, "timeout"
    except FileNotFoundError:
        return False, "request_error"
    except Exception:
        return False, "request_error"

    output = f"{proc.stdout}\n{proc.stderr}".lower()
    if "timed out" in output:
        return False, "timeout"
    if "mail exchanger" in output or "mx preference" in output:
        return True, None
    if "non-existent domain" in output or "can't find" in output or "no answer" in output:
        return False, "no_mx_records"
    return False, "no_mx_records"


def _has_mx_records(domain: str) -> Tuple[bool, Optional[str]]:
    """Validate MX presence via dnspython if available, else nslookup."""
    clean_domain = normalize_text(domain).lower().strip(".")
    if not clean_domain:
        return False, "no_mx_records"

    try:
        import dns.resolver  # type: ignore

        resolver = dns.resolver.Resolver()
        resolver.lifetime = 5
        answers = resolver.resolve(clean_domain, "MX")
        if answers and len(answers) > 0:
            return True, None
        return False, "no_mx_records"
    except Exception as exc:
        if exc.__class__.__name__ in {"NXDOMAIN", "NoAnswer", "NoNameservers"}:
            return False, "no_mx_records"
        if exc.__class__.__name__ in {"LifetimeTimeout", "Timeout"}:
            return False, "timeout"
        return _mx_lookup_with_nslookup(clean_domain)


def _domain_validation_reason(
    *,
    status_code: Optional[int] = None,
    reject_reason: Optional[str] = None,
    valid: bool = False,
) -> str:
    """Normalize internal rejection reasons to stable output reasons."""
    if valid:
        return "valid"
    if reject_reason == "no_mx_records":
        return "no_mx_records"
    if reject_reason == "disposable_domain":
        return "disposable_domain"
    if reject_reason == "parking_page":
        return "parked_domain"
    if reject_reason == "ssl_error":
        return "ssl_error"
    if reject_reason == "timeout":
        return "timeout"
    if status_code == 404 or reject_reason == "404":
        return "http_404"
    return normalize_text(reject_reason or "request_error").lower().replace(" ", "_")


def _build_valid_result(
    clean_domain: str,
    status_code: Optional[int],
    final_url: Optional[str],
    construction_keyword_match: bool = False,
) -> Dict[str, object]:
    return {
        "domain": clean_domain,
        "domain_valid": True,
        "mx_valid": True,
        "domain_validation_reason": "valid",
        "status_code": status_code,
        "final_url": final_url,
        "valid": True,
        "reject_reason": None,
        "construction_keyword_match": bool(construction_keyword_match),
    }


def _maybe_accept_via_mx_only(
    clean_domain: str,
    *,
    status_code: Optional[int],
    final_url: Optional[str],
    reject_reason: str,
    company_context: str = "",
) -> Optional[Dict[str, object]]:
    """
    Accept domains as email-usable when web checks fail but MX is valid.
    This is intentionally narrow to improve deliverability-focused workflows.
    """
    mx_valid, _ = _has_mx_records(clean_domain)
    if not mx_valid:
        return None

    if _is_ambiguous_mx_only_match(company_context, clean_domain):
        log.info(
            "MX-only rejected as ambiguous | company=%s domain=%s web_reject_reason=%s status_code=%s",
            normalize_text(company_context).lower() or "none",
            clean_domain,
            reject_reason,
            status_code,
        )
        return {
            "domain": clean_domain,
            "domain_valid": False,
            "mx_valid": True,
            "domain_validation_reason": "ambiguous_generic_domain",
            "status_code": status_code,
            "final_url": final_url,
            "valid": False,
            "reject_reason": "ambiguous_generic_domain",
            "construction_keyword_match": False,
        }

    log.info(
        "MX-only acceptance | domain=%s web_reject_reason=%s status_code=%s",
        clean_domain,
        reject_reason,
        status_code,
    )
    return _build_valid_result(
        clean_domain=clean_domain,
        status_code=status_code,
        final_url=final_url,
        construction_keyword_match=False,
    )


def validate_domain(domain: str, company_context: str = "") -> Dict[str, object]:
    """Validate a domain candidate with HEAD check, retry-on-403, and content sanity checks."""
    clean_domain = normalize_text(domain).lower()
    if not clean_domain:
        return {
            "domain": clean_domain,
            "domain_valid": False,
            "mx_valid": False,
            "domain_validation_reason": "empty_domain",
            "status_code": None,
            "final_url": None,
            "valid": False,
            "reject_reason": "empty_domain",
            "construction_keyword_match": False,
        }
    if _is_disposable_domain(clean_domain):
        return {
            "domain": clean_domain,
            "domain_valid": False,
            "mx_valid": False,
            "domain_validation_reason": "disposable_domain",
            "status_code": None,
            "final_url": None,
            "valid": False,
            "reject_reason": "disposable_domain",
            "construction_keyword_match": False,
        }

    test_url = f"https://{clean_domain}"
    headers = {"User-Agent": PRIMARY_UA}
    status_code: Optional[int] = None
    final_url: Optional[str] = None

    try:
        response = requests.head(
            test_url,
            timeout=5,
            headers=headers,
            allow_redirects=False,
        )
        status_code = response.status_code
        final_url = response.url

        if status_code == 403:
            response = requests.head(
                test_url,
                timeout=5,
                headers={"User-Agent": FALLBACK_UA},
                allow_redirects=False,
            )
            status_code = response.status_code
            final_url = response.url
        if status_code in {301, 302}:
            redirect_to = normalize_text(response.headers.get("Location", ""))
            if redirect_to:
                try:
                    follow_resp = requests.head(
                        redirect_to,
                        timeout=5,
                        headers=headers,
                        allow_redirects=False,
                    )
                    status_code = follow_resp.status_code
                    final_url = follow_resp.url or redirect_to
                except Exception:
                    final_url = redirect_to
    except requests.exceptions.SSLError:
        mx_only = _maybe_accept_via_mx_only(
            clean_domain,
            status_code=None,
            final_url=None,
            reject_reason="ssl_error",
            company_context=company_context,
        )
        if mx_only is not None:
            return mx_only
        return {
            "domain": clean_domain,
            "domain_valid": False,
            "mx_valid": False,
            "domain_validation_reason": "ssl_error",
            "status_code": None,
            "final_url": None,
            "valid": False,
            "reject_reason": "ssl_error",
            "construction_keyword_match": False,
        }
    except requests.exceptions.Timeout:
        mx_only = _maybe_accept_via_mx_only(
            clean_domain,
            status_code=None,
            final_url=None,
            reject_reason="timeout",
            company_context=company_context,
        )
        if mx_only is not None:
            return mx_only
        return {
            "domain": clean_domain,
            "domain_valid": False,
            "mx_valid": False,
            "domain_validation_reason": "timeout",
            "status_code": None,
            "final_url": None,
            "valid": False,
            "reject_reason": "timeout",
            "construction_keyword_match": False,
        }
    except Exception:
        mx_only = _maybe_accept_via_mx_only(
            clean_domain,
            status_code=None,
            final_url=None,
            reject_reason="request_error",
            company_context=company_context,
        )
        if mx_only is not None:
            return mx_only
        return {
            "domain": clean_domain,
            "domain_valid": False,
            "mx_valid": False,
            "domain_validation_reason": "request_error",
            "status_code": None,
            "final_url": None,
            "valid": False,
            "reject_reason": "request_error",
            "construction_keyword_match": False,
        }

    if status_code not in VALID_STATUS_CODES:
        reason = "status_rejected"
        if status_code in REJECT_STATUS_CODES:
            reason = str(status_code)
        if status_code == 403:
            mx_only = _maybe_accept_via_mx_only(
                clean_domain,
                status_code=status_code,
                final_url=final_url,
                reject_reason=reason,
                company_context=company_context,
            )
            if mx_only is not None:
                return mx_only
        return {
            "domain": clean_domain,
            "domain_valid": False,
            "mx_valid": False,
            "domain_validation_reason": _domain_validation_reason(
                status_code=status_code, reject_reason=reason, valid=False
            ),
            "status_code": status_code,
            "final_url": final_url,
            "valid": False,
            "reject_reason": reason,
            "construction_keyword_match": False,
        }

    content_check = _check_parking_and_keywords(final_url or test_url)
    if content_check.get("error") in {"ssl_error", "timeout"}:
        reason = str(content_check["error"])
        return {
            "domain": clean_domain,
            "domain_valid": False,
            "mx_valid": False,
            "domain_validation_reason": _domain_validation_reason(
                status_code=status_code, reject_reason=reason, valid=False
            ),
            "status_code": status_code,
            "final_url": final_url,
            "valid": False,
            "reject_reason": reason,
            "construction_keyword_match": False,
        }
    if content_check.get("parking"):
        reason = "parking_page"
        parked_evidence = normalize_text(content_check.get("parked_evidence", ""))
        log.info(
            "Parked-domain evidence | domain=%s evidence=%s",
            clean_domain,
            parked_evidence or "none",
        )
        return {
            "domain": clean_domain,
            "domain_valid": False,
            "mx_valid": False,
            "domain_validation_reason": _domain_validation_reason(
                status_code=status_code, reject_reason=reason, valid=False
            ),
            "status_code": status_code,
            "final_url": final_url,
            "valid": False,
            "reject_reason": reason,
            "construction_keyword_match": False,
        }

    if _is_ambiguous_mx_only_match(company_context, clean_domain) and not bool(content_check.get("construction_keyword_match", False)):
        return {
            "domain": clean_domain,
            "domain_valid": False,
            "mx_valid": False,
            "domain_validation_reason": "ambiguous_generic_domain",
            "status_code": status_code,
            "final_url": final_url,
            "valid": False,
            "reject_reason": "ambiguous_generic_domain",
            "construction_keyword_match": False,
        }

    mx_valid, mx_reason = _has_mx_records(clean_domain)
    if not mx_valid:
        reason = mx_reason or "no_mx_records"
        return {
            "domain": clean_domain,
            "domain_valid": False,
            "mx_valid": False,
            "domain_validation_reason": _domain_validation_reason(
                status_code=status_code, reject_reason=reason, valid=False
            ),
            "status_code": status_code,
            "final_url": final_url,
            "valid": False,
            "reject_reason": reason,
            "construction_keyword_match": bool(content_check.get("construction_keyword_match", False)),
        }

    return _build_valid_result(
        clean_domain=clean_domain,
        status_code=status_code,
        final_url=final_url,
        construction_keyword_match=bool(content_check.get("construction_keyword_match", False)),
    )


def discover_domain(company_name: str, state_abbr: str = "") -> Dict[str, object]:
    """Discover the most likely valid domain for a company from generated variants."""
    cleaned_name = clean_company_name(company_name)
    slug = normalize_company_slug(cleaned_name)
    variants = generate_domain_variants(slug, cleaned_name, state_abbr=state_abbr)
    if not variants:
        return {
            "domain": None,
            "domain_valid": False,
            "mx_valid": False,
            "domain_validation_reason": "no_variants",
            "valid": False,
            "reject_reason": "no_variants",
            "construction_keyword_match": False,
        }

    best_valid: Optional[Dict[str, object]] = None
    for candidate in variants:
        result = validate_domain(candidate, company_context=cleaned_name)
        if not result.get("valid"):
            continue
        if best_valid is None:
            best_valid = result
            continue
        # Prefer construction keyword relevance when multiple variants validate.
        if bool(result.get("construction_keyword_match")) and not bool(best_valid.get("construction_keyword_match")):
            best_valid = result

    if best_valid is not None:
        return best_valid
    return {
        "domain": None,
        "domain_valid": False,
        "mx_valid": False,
        "domain_validation_reason": "no_valid_domain",
        "valid": False,
        "reject_reason": "no_valid_domain",
        "construction_keyword_match": False,
    }


def _safe_int(value: object, default: int = 0) -> int:
    try:
        if value is None:
            return default
        text = normalize_text(value)
        if not text:
            return default
        return int(float(text))
    except Exception:
        return default


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        text = normalize_text(value)
        if not text:
            return default
        return float(text)
    except Exception:
        return default


FREE_ISP_EMAIL_DOMAIN_SUFFIXES = {
    "gmail.com",
    "yahoo.com",
    "hotmail.com",
    "outlook.com",
    "live.com",
    "live.ca",
    "msn.com",
    "icloud.com",
    "aol.com",
    "proton.me",
    "protonmail.com",
    "shaw.ca",
    "telus.net",
    "sasktel.net",
}


def _is_free_isp_seed_domain(domain: str) -> bool:
    clean = normalize_text(domain).lower().strip()
    if not clean:
        return True
    return any(clean == bad or clean.endswith(f".{bad}") for bad in FREE_ISP_EMAIL_DOMAIN_SUFFIXES)


def _ci_seed_row_score(row: pd.Series) -> int:
    domain_confidence_raw = normalize_text(row.get("domain_confidence", "")).lower()
    domain_confidence_numeric = _safe_float(domain_confidence_raw, default=-1.0)
    source_support = _safe_int(row.get("source_support_count", 0), default=0)

    if domain_confidence_numeric >= 0:
        if domain_confidence_numeric >= 0.9:
            base = 3
        elif domain_confidence_numeric >= 0.75:
            base = 2
        elif domain_confidence_numeric >= 0.6:
            base = 1
        else:
            base = 0
    else:
        base = {"high": 3, "medium": 2, "low": 1}.get(domain_confidence_raw, 0)

    support_bonus = 2 if source_support >= 10 else 1 if source_support >= 3 else 0

    conflict_raw = normalize_text(row.get("conflict_flag", "")).lower()
    conflict_flag = conflict_raw in {"1", "true", "yes", "y", "t"}
    notes = " ".join(
        [
            normalize_text(row.get("domain_evidence_notes", "")).lower(),
            normalize_text(row.get("quality_notes", "")).lower(),
        ]
    )
    has_conflict_signal = conflict_flag or ("conflict" in notes)

    weak_signal = base <= 1 and source_support < 3

    score = base + support_bonus
    if has_conflict_signal:
        score -= 2
    if weak_signal:
        score -= 1
    return score


def _build_ci_seed_domain_map(df: pd.DataFrame) -> Dict[str, str]:
    if df.empty:
        return {}

    per_company_domain_scores: Dict[str, Dict[str, int]] = {}
    for _, row in df.iterrows():
        company_name = (
            normalize_text(row.get("normalized_company_name", ""))
            or normalize_text(row.get("company_name", ""))
            or normalize_text(row.get("cleaned_company_name", ""))
        )
        key = normalize_company_slug(clean_company_name(company_name))
        domain = normalize_text(row.get("preferred_domain", "")).lower().strip()
        if not domain:
            domain = normalize_text(row.get("contractor_domain", "")).lower().strip()
        domain = domain.split("|")[0].strip()
        if not key or not domain or "." not in domain:
            continue
        if _is_free_isp_seed_domain(domain):
            continue

        score = _ci_seed_row_score(row)
        domain_scores = per_company_domain_scores.setdefault(key, {})
        current = domain_scores.get(domain)
        if current is None or score > current:
            domain_scores[domain] = score

    resolved: Dict[str, str] = {}
    for key, domain_scores in per_company_domain_scores.items():
        ranked = sorted(domain_scores.items(), key=lambda item: (item[1], len(item[0])), reverse=True)
        if not ranked:
            continue
        top_domain, top_score = ranked[0]
        second_score = ranked[1][1] if len(ranked) > 1 else -999

        if top_score < 3:
            continue
        if len(ranked) > 1 and (top_score - second_score) < 1:
            continue

        resolved[key] = top_domain

    return resolved


def _load_ci_seed_domain_map() -> Dict[str, str]:
    seed_frames: List[pd.DataFrame] = []
    for path in (CI_SEED_LOCAL_CSV_PATH, CI_SEED_EXPORT_XLSX_PATH):
        if not path.exists():
            continue
        try:
            if path.suffix.lower() == ".xlsx":
                seed_frames.append(pd.read_excel(path))
            else:
                seed_frames.append(pd.read_csv(path))
        except Exception as exc:
            log.warning("Failed loading CI seed file %s: %s", path, exc)

    if not seed_frames:
        return {}

    combined = pd.concat(seed_frames, ignore_index=True, sort=False)
    mapping = _build_ci_seed_domain_map(combined)
    log.info("Loaded CI domain seed intelligence | seed_rows=%d mapped_companies=%d", int(len(combined)), int(len(mapping)))
    return mapping


def _load_name_domain_map() -> Dict[str, str]:
    """Load a normalized company->domain mapping from existing enriched and seed files."""
    mapping: Dict[str, str] = {}
    if ENRICHED_PATH.exists():
        df = pd.read_csv(ENRICHED_PATH)
        if {"contractor_name_normalized", "contractor_domain"}.issubset(df.columns):
            for _, row in df.iterrows():
                name = normalize_company_slug(row.get("contractor_name_normalized", ""))
                cleaned_name = normalize_company_slug(clean_company_name(row.get("contractor_name_normalized", "")))
                domain = normalize_text(row.get("contractor_domain", "")).lower().split("|")[0].strip()
                if name and domain:
                    mapping[name] = domain
                if cleaned_name and domain:
                    mapping[cleaned_name] = domain
    if SEED_PATH.exists():
        df = pd.read_csv(SEED_PATH)
        if {"contractor_name_normalized", "contractor_domain"}.issubset(df.columns):
            for _, row in df.iterrows():
                name = normalize_company_slug(row.get("contractor_name_normalized", ""))
                cleaned_name = normalize_company_slug(clean_company_name(row.get("contractor_name_normalized", "")))
                domain = normalize_text(row.get("contractor_domain", "")).lower().split("|")[0].strip()
                if name and domain and name not in mapping:
                    mapping[name] = domain
                if cleaned_name and domain and cleaned_name not in mapping:
                    mapping[cleaned_name] = domain
    return mapping


def _fuzzy_existing_map_domain(cleaned_company_name: str, existing_map: Dict[str, str]) -> Optional[str]:
    """
    Resolve near-match company slugs from local known-domain map.
    High-confidence only: tight similarity + shape constraints.
    """
    slug = normalize_company_slug(cleaned_company_name)
    if not slug or not existing_map:
        return None

    best_key = ""
    best_ratio = 0.0
    for known_slug in existing_map.keys():
        if not known_slug:
            continue
        if abs(len(known_slug) - len(slug)) > 4:
            continue
        if len(slug) >= 4 and len(known_slug) >= 4 and slug[:3] != known_slug[:3]:
            continue
        ratio = SequenceMatcher(None, slug, known_slug).ratio()
        if ratio > best_ratio:
            best_ratio = ratio
            best_key = known_slug

    if not best_key:
        return None
    if best_ratio < 0.88:
        return None
    if not (slug in best_key or best_key in slug or slug[:5] == best_key[:5]):
        return None
    return existing_map.get(best_key)


def _is_excluded_search_domain(domain: str) -> bool:
    clean = normalize_text(domain).lower().strip(".")
    if not clean:
        return True
    return any(clean == bad or clean.endswith(f".{bad}") for bad in SEARCH_EXCLUDED_DOMAINS)


def _domain_from_url(candidate_url: str) -> str:
    parsed = urlparse(candidate_url)
    netloc = normalize_text(parsed.netloc).lower().strip()
    if not netloc:
        return ""
    netloc = netloc.split(":")[0].strip().lstrip("www.")
    return netloc


def _extract_search_target_url(href: str) -> str:
    clean_href = normalize_text(href).strip()
    if not clean_href:
        return ""
    parsed = urlparse(clean_href)
    query = parse_qs(parsed.query)
    for key in ("uddg", "u", "url"):
        if key in query and query[key]:
            return unquote(query[key][0])
    if parsed.scheme in {"http", "https"}:
        return clean_href
    return ""


def _extract_candidate_domains_from_search_html(html: str) -> List[str]:
    soup = BeautifulSoup(html or "", "lxml")
    domains: List[str] = []
    seen = set()
    for a in soup.select("a[href]"):
        href = normalize_text(a.get("href", "")).strip()
        if not href:
            continue
        target_url = _extract_search_target_url(href)
        if not target_url:
            continue
        domain = _domain_from_url(target_url)
        if not domain or _is_excluded_search_domain(domain):
            continue
        if domain in seen:
            continue
        seen.add(domain)
        domains.append(domain)
    return domains


def _normalize_domain_root_for_match(domain: str) -> str:
    root = normalize_text(domain).lower().split(".")[0]
    root = re.sub(r"[^a-z0-9]", "", root)
    return root


def _company_distinctive_tokens(cleaned_company_name: str) -> List[str]:
    tokens = [
        t
        for t in re.findall(r"[a-z0-9]+", normalize_text(cleaned_company_name).lower())
        if t and t not in COMPANY_GENERIC_TOKENS
    ]
    return tokens


def _is_ambiguous_mx_only_match(cleaned_company_name: str, candidate_domain: str) -> bool:
    """Conservative guardrail for MX-only acceptance on ambiguous generic domains."""
    company = normalize_text(cleaned_company_name).lower().strip()
    if not company:
        return False

    domain_root = _normalize_domain_root_for_match(candidate_domain)
    if not domain_root:
        return False

    tokens = _company_distinctive_tokens(company)
    if not tokens:
        return True

    # Known high-risk generic roots observed as false positives.
    if domain_root in AMBIGUOUS_GENERIC_ROOTS and domain_root in tokens:
        return True

    # Very short roots accepted via MX-only are usually too ambiguous.
    if domain_root == ''.join(tokens) and len(domain_root) <= 3:
        return True

    # Single-token companies with short/generic root are often mismatches.
    if len(tokens) == 1 and tokens[0] == domain_root and len(domain_root) <= 5:
        return True

    return False


def _company_tokens_for_match(cleaned_company_name: str) -> List[str]:
    tokens = [
        t
        for t in re.findall(r"[a-z0-9]+", normalize_text(cleaned_company_name).lower())
        if t and t not in SEARCH_TOKEN_STOPWORDS and len(t) >= 3
    ]
    return tokens


def _search_candidate_score(cleaned_company_name: str, candidate_domain: str) -> float:
    """Score search candidate domains against company tokens for rank ordering."""
    company_slug = normalize_company_slug(cleaned_company_name)
    domain_root_raw = normalize_text(candidate_domain).lower().split(".")[0]
    domain_root = re.sub(r"[^a-z0-9]", "", domain_root_raw)
    if not company_slug or not domain_root:
        return 0.0

    ratio = SequenceMatcher(None, company_slug, domain_root).ratio()
    company_tokens = _company_tokens_for_match(cleaned_company_name)

    token_hit_count = sum(1 for t in company_tokens if t in domain_root)
    token_coverage = token_hit_count / max(1, len(company_tokens))

    contains_slug_bonus = 0.30 if (company_slug in domain_root or domain_root in company_slug) else 0.0
    prefix_bonus = 0.18 if company_slug[:4] and domain_root.startswith(company_slug[:4]) else 0.0
    token_bonus = 0.50 * token_coverage

    # Penalize weak roots that share little structure with company naming.
    mismatch_penalty = 0.20 if token_hit_count == 0 and ratio < 0.70 else 0.0

    return ratio + contains_slug_bonus + prefix_bonus + token_bonus - mismatch_penalty


def _build_search_queries(cleaned_company_name: str, project_city: str = "", project_state: str = "") -> List[str]:
    name = normalize_text(cleaned_company_name).strip()
    city = normalize_text(project_city).strip()
    state = normalize_text(project_state).strip()
    if not name:
        return []

    base = " ".join([x for x in [name, city, state] if x]).strip()
    queries: List[str] = [base] if base else [name]
    if city or state:
        queries.append(" ".join([x for x in [name, state] if x]).strip())
        queries.append(f'"{name}" {city} {state}'.strip())
    queries.append(f'"{name}" official site')

    low = name.lower()
    if not any(k in low for k in ["construction", "contractor", "builders", "building"]):
        queries.append(f"{base or name} construction contractor")

    deduped: List[str] = []
    seen = set()
    for q in queries:
        qn = normalize_text(q).strip()
        if qn and qn not in seen:
            seen.add(qn)
            deduped.append(qn)
    return deduped


def _resolve_domain_via_search(cleaned_company_name: str, project_city: str = "", project_state: str = "") -> Dict[str, object]:
    """
    Search fallback for unresolved companies.
    Returns selected query/candidate plus validation result if successful.
    """
    cache_key = "|".join([
        normalize_text(cleaned_company_name).lower(),
        normalize_text(project_city).lower(),
        normalize_text(project_state).lower(),
    ])
    if cache_key in _SEARCH_RESOLVE_CACHE:
        return dict(_SEARCH_RESOLVE_CACHE[cache_key])

    queries = _build_search_queries(cleaned_company_name, project_city, project_state)
    for query in queries[:SEARCH_FALLBACK_QUERY_BUDGET]:
        try:
            resp = requests.get(
                f"{SEARCH_BASE_URL}?q={quote_plus(query)}",
                headers={"User-Agent": PRIMARY_UA},
                timeout=8,
                allow_redirects=True,
            )
            html = resp.text if resp.ok else ""
        except Exception:
            html = ""
        domains = _extract_candidate_domains_from_search_html(html)
        ranked = sorted(
            domains,
            key=lambda d: _search_candidate_score(cleaned_company_name, d),
            reverse=True,
        )
        for candidate in ranked[:SEARCH_FALLBACK_CANDIDATE_BUDGET]:
            score = _search_candidate_score(cleaned_company_name, candidate)
            if score < 0.45:
                log.info(
                    "Search fallback skip-low-score | cleaned_company=%s search_query=%s search_candidate_domain=%s score=%.3f",
                    cleaned_company_name,
                    query,
                    candidate,
                    score,
                )
                continue
            validation = validate_domain(candidate, company_context=cleaned_company_name)
            log.info(
                "Search fallback check | cleaned_company=%s search_query=%s search_candidate_domain=%s score=%.3f valid=%s reason=%s",
                cleaned_company_name,
                query,
                candidate,
                score,
                bool(validation.get("domain_valid", validation.get("valid", False))),
                normalize_text(validation.get("domain_validation_reason", validation.get("reject_reason", ""))).lower(),
            )
            if bool(validation.get("domain_valid", validation.get("valid", False))):
                out = {
                    "result": validation,
                    "search_query": query,
                    "search_candidate_domain": candidate,
                }
                _SEARCH_RESOLVE_CACHE[cache_key] = dict(out)
                return out
    out = {"result": None, "search_query": "", "search_candidate_domain": ""}
    _SEARCH_RESOLVE_CACHE[cache_key] = dict(out)
    return out




def _derive_domain_confidence(
    *,
    is_domain_valid: bool,
    source: str,
    cleaned_company_name: str,
    domain: str,
    result: Dict[str, object],
) -> str:
    """Small stable downstream confidence label for resolved domains."""
    if not is_domain_valid or not domain:
        return "low"

    if normalize_text(result.get("domain_validation_reason", "")) not in {"", "valid"}:
        return "low"

    if source == "existing_input_validated":
        return "high"

    if source in {"existing_map_validated", "existing_map_validated_raw", "ci_seed_exact_validated", "ci_seed_exact_validated_raw", "ci_seed_fuzzy_validated"}:
        return "medium"

    if source in {"variant_discovery", "fuzzy_existing_map_validated", "search_fallback_validated", "search_fallback_slimmed_name_validated"}:
        company_tokens = _company_distinctive_tokens(cleaned_company_name)
        if len("".join(company_tokens)) <= 2:
            return "low"
        if _is_ambiguous_mx_only_match(cleaned_company_name, domain):
            return "low"
        score = _search_candidate_score(cleaned_company_name, domain)
        if score < 0.75:
            return "low"
        if score >= 1.05 or bool(result.get("construction_keyword_match", False)):
            return "high"
        return "medium"

    return "medium"


def discover_company_domains(companies_df: pd.DataFrame) -> pd.DataFrame:
    """Resolve domains for company rows while preserving monday people pipeline compatibility."""
    columns = [
        "contractor_name_raw",
        "contractor_name_normalized",
        "cleaned_company_name",
        "contractor_domain",
        "domain",
        "project_city",
        "project_state",
        "best_project_description",
        "source_rank_tier",
        "domain_valid",
        "mx_valid",
        "domain_validation_reason",
        "domain_confidence",
        "construction_keyword_match",
        "domain_discovery_source",
        "discovery_method",
        "search_query",
        "search_candidate_domain",
    ]
    if companies_df.empty:
        return pd.DataFrame(columns=columns)

    existing_map = _load_name_domain_map()
    ci_seed_map = _load_ci_seed_domain_map()
    rows: List[Dict[str, object]] = []
    search_fallback_attempts = 0
    for _, row in companies_df.iterrows():
        raw_company_name = normalize_text(row.get("contractor_name_normalized", ""))
        company_name = raw_company_name.lower()
        cleaned_company_name = clean_company_name(raw_company_name)
        cleaned_company_name_normalized = cleaned_company_name.lower()
        state = normalize_text(row.get("project_state", "")).upper()
        provided_domain = normalize_text(row.get("contractor_domain", "")).lower().split("|")[0].strip()
        key = normalize_company_slug(cleaned_company_name_normalized)
        raw_key = normalize_company_slug(company_name)
        project_city = normalize_text(row.get("project_city", ""))

        result: Dict[str, object]
        source = "existing_input"
        search_query = ""
        search_candidate_domain = ""
        if provided_domain:
            result = validate_domain(provided_domain, company_context=cleaned_company_name_normalized or company_name)
            source = "existing_input_validated"
        elif key in ci_seed_map:
            result = validate_domain(ci_seed_map[key], company_context=cleaned_company_name_normalized or company_name)
            source = "ci_seed_exact_validated"
        elif raw_key in ci_seed_map:
            result = validate_domain(ci_seed_map[raw_key], company_context=cleaned_company_name_normalized or company_name)
            source = "ci_seed_exact_validated_raw"
        else:
            ci_fuzzy_domain = _fuzzy_existing_map_domain(cleaned_company_name_normalized or company_name, ci_seed_map)
            if ci_fuzzy_domain:
                result = validate_domain(ci_fuzzy_domain, company_context=cleaned_company_name_normalized or company_name)
                source = "ci_seed_fuzzy_validated"
            elif key in existing_map:
                result = validate_domain(existing_map[key], company_context=cleaned_company_name_normalized or company_name)
                source = "existing_map_validated"
            elif raw_key in existing_map:
                result = validate_domain(existing_map[raw_key], company_context=cleaned_company_name_normalized or company_name)
                source = "existing_map_validated_raw"
            else:
                result = discover_domain(cleaned_company_name_normalized or company_name, state_abbr=state)
                source = "variant_discovery"
                if not bool(result.get("domain_valid", result.get("valid", False))) and str(
                    result.get("domain_validation_reason", "")
                ) in {"no_valid_domain", "no_variants"}:
                    fuzzy_domain = _fuzzy_existing_map_domain(cleaned_company_name_normalized or company_name, existing_map)
                    if fuzzy_domain:
                        result = validate_domain(fuzzy_domain, company_context=cleaned_company_name_normalized or company_name)
                        source = "fuzzy_existing_map_validated"
            if not bool(result.get("domain_valid", result.get("valid", False))) and str(
                result.get("domain_validation_reason", "")
            ) in {"no_valid_domain", "no_variants"} and search_fallback_attempts < MAX_SEARCH_FALLBACK_ATTEMPTS:
                search_fallback_attempts += 1
                search_res = _resolve_domain_via_search(cleaned_company_name_normalized or company_name, project_city, state)
                search_query = normalize_text(search_res.get("search_query", ""))
                search_candidate_domain = normalize_text(search_res.get("search_candidate_domain", "")).lower()
                if search_res.get("result"):
                    result = search_res["result"]
                    source = "search_fallback_validated"
                log.info(
                    "Search fallback result | raw_company=%s cleaned_company=%s search_query=%s search_candidate_domain=%s final_valid=%s final_reason=%s",
                    raw_company_name,
                    cleaned_company_name_normalized,
                    search_query or "none",
                    search_candidate_domain or "none",
                    bool(result.get("domain_valid", result.get("valid", False))),
                    normalize_text(result.get("domain_validation_reason", result.get("reject_reason", ""))).lower(),
                )
                if not bool(result.get("domain_valid", result.get("valid", False))) and cleaned_company_name_normalized:
                    slimmed_company_name = re.sub(
                        r"\b(inc|llc|corp|co|company|group|services|solutions)\b",
                        " ",
                        cleaned_company_name_normalized,
                        flags=re.IGNORECASE,
                    )
                    slimmed_company_name = re.sub(r"\s+", " ", slimmed_company_name).strip()
                    if slimmed_company_name and slimmed_company_name != cleaned_company_name_normalized and search_fallback_attempts < MAX_SEARCH_FALLBACK_ATTEMPTS:
                        search_fallback_attempts += 1
                        search_res_2 = _resolve_domain_via_search(slimmed_company_name, project_city, state)
                        search_query = normalize_text(search_res_2.get("search_query", ""))
                        search_candidate_domain = normalize_text(search_res_2.get("search_candidate_domain", "")).lower()
                        if search_res_2.get("result"):
                            result = search_res_2["result"]
                            source = "search_fallback_slimmed_name_validated"
                        log.info(
                            "Search fallback slimmed-name result | raw_company=%s cleaned_company=%s slimmed_company=%s search_query=%s search_candidate_domain=%s final_valid=%s final_reason=%s",
                            raw_company_name,
                            cleaned_company_name_normalized,
                            slimmed_company_name,
                            search_query or "none",
                            search_candidate_domain or "none",
                            bool(result.get("domain_valid", result.get("valid", False))),
                            normalize_text(result.get("domain_validation_reason", result.get("reject_reason", ""))).lower(),
                        )

        raw_domain = normalize_text(
            result.get("domain")
            or provided_domain
            or ci_seed_map.get(key, "")
            or ci_seed_map.get(raw_key, "")
            or existing_map.get(key, "")
            or existing_map.get(raw_key, "")
        ).lower()
        is_domain_valid = bool(result.get("domain_valid", result.get("valid", False)))
        domain = raw_domain if is_domain_valid else ""
        domain_validation_reason = normalize_text(
            result.get(
                "domain_validation_reason",
                _domain_validation_reason(
                    status_code=result.get("status_code"),
                    reject_reason=normalize_text(result.get("reject_reason", "")),
                    valid=is_domain_valid,
                ),
            )
        ).lower()
        mx_valid = bool(result.get("mx_valid", False))
        domain_confidence = _derive_domain_confidence(
            is_domain_valid=is_domain_valid,
            source=source,
            cleaned_company_name=cleaned_company_name_normalized or company_name,
            domain=raw_domain,
            result=result,
        )

        log.info(
            "Domain validation result | raw_company=%s cleaned_company=%s domain_candidate=%s approved=%s domain_valid=%s mx_valid=%s confidence=%s reason=%s source=%s",
            raw_company_name,
            cleaned_company_name_normalized,
            raw_domain or "none",
            domain or "none",
            is_domain_valid,
            mx_valid,
            domain_confidence,
            domain_validation_reason,
            source,
        )

        rows.append(
            {
                "contractor_name_raw": raw_company_name,
                "contractor_name_normalized": company_name,
                "cleaned_company_name": cleaned_company_name_normalized,
                "contractor_domain": domain,
                "domain": domain,
                "project_city": normalize_text(row.get("project_city", "")),
                "project_state": normalize_text(row.get("project_state", "")),
                "best_project_description": normalize_text(row.get("best_project_description", "")),
                "source_rank_tier": normalize_text(row.get("source_rank_tier", "")) or "ranked",
                "domain_valid": is_domain_valid,
                "mx_valid": mx_valid,
                "domain_validation_reason": domain_validation_reason,
                "domain_confidence": domain_confidence,
                "construction_keyword_match": bool(result.get("construction_keyword_match", False)),
                "domain_discovery_source": source,
                "discovery_method": source,
                "search_query": search_query,
                "search_candidate_domain": search_candidate_domain,
            }
        )

    out = pd.DataFrame(rows, columns=columns)
    out = out.sort_values(by=["contractor_name_normalized"]).drop_duplicates(subset=["contractor_name_normalized"], keep="first")
    return out
