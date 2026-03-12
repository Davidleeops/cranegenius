from __future__ import annotations

import logging
import re
from typing import Dict, List, Optional, Sequence, Tuple

import pandas as pd

from .utils import normalize_text

log = logging.getLogger("cranegenius.people_email_generator")

MAX_PATTERNS_PER_PERSON = 8
DEFAULT_PATTERNS_PER_PERSON = 2

PATTERN_SPECS: List[Tuple[str, float]] = [
    ("first.last", 0.96),
    ("flast", 0.93),
    ("firstlast", 0.91),
    ("f.lastname", 0.88),
    ("first_last", 0.86),
    ("firstname.lastname", 0.82),
    ("first", 0.76),
    ("last.first", 0.70),
]

SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v"}


COMPANY_GENERIC_TOKENS = {"construction", "contracting", "contractor", "builders", "building", "services", "service", "group", "inc", "llc", "co", "company", "the", "and"}


def _company_tokens(company: str) -> List[str]:
    return [
        tok for tok in re.findall(r"[a-z0-9]+", normalize_text(company).lower())
        if tok and tok not in COMPANY_GENERIC_TOKENS and len(tok) >= 2
    ]


def _domain_root(domain: str) -> str:
    return normalize_text(domain).lower().split(".")[0]


def _default_pattern_specs_for_person(company: str, domain: str) -> List[Tuple[str, float]]:
    """Heuristic preference for top patterns on this company/domain pair."""
    root = _domain_root(domain)
    tokens = _company_tokens(company)
    base = PATTERN_SPECS.copy()

    initials = "".join(tok[0] for tok in tokens[:6]) if tokens else ""
    has_token = any(tok in root for tok in tokens)
    has_abbrev = bool(initials) and initials in root

    if has_abbrev and not has_token:
        preferred = ["flast", "first.last"]
    elif has_token:
        preferred = ["first.last", "flast"]
    else:
        preferred = ["first.last", "flast"]

    # keep full list available; only top ordering changes by heuristic
    ordered = sorted(base, key=lambda p: (preferred.index(p[0]) if p[0] in preferred else 99,))
    return ordered


def _normalize_domain(domain: str) -> str:
    clean = normalize_text(domain).lower().strip()
    clean = clean.replace("http://", "").replace("https://", "")
    clean = clean.split("/")[0].strip(".")
    return clean


def _normalize_name_token(token: str) -> str:
    clean = normalize_text(token).lower()
    # Keep letters only for predictable local-part generation.
    return re.sub(r"[^a-z]", "", clean)


def _extract_name_parts(row: pd.Series) -> Tuple[str, str, str]:
    first_raw = normalize_text(row.get("first_name", ""))
    last_raw = normalize_text(row.get("last_name", ""))
    full_raw = normalize_text(row.get("full_name", ""))

    first = _normalize_name_token(first_raw)
    last = _normalize_name_token(last_raw)

    if (not first or not last) and full_raw:
        tokens_raw = [t for t in re.split(r"\s+", full_raw.strip()) if t]
        tokens_norm = [_normalize_name_token(t) for t in tokens_raw]
        tokens_norm = [t for t in tokens_norm if t]
        if len(tokens_norm) >= 2:
            if tokens_norm[-1] in SUFFIXES and len(tokens_norm) >= 3:
                first = first or tokens_norm[0]
                last = last or tokens_norm[-2]
            else:
                first = first or tokens_norm[0]
                last = last or tokens_norm[-1]

    full_name = full_raw
    if not full_name and first and last:
        full_name = f"{first.title()} {last.title()}"

    return first, last, full_name


def _build_local_part(pattern: str, first: str, last: str) -> Optional[str]:
    if not (first and last):
        return None
    if pattern == "first.last":
        return f"{first}.{last}"
    if pattern == "firstname.lastname":
        return f"{first}.{last}"
    if pattern == "first":
        return first
    if pattern == "flast":
        return f"{first[0]}{last}"
    if pattern == "firstlast":
        return f"{first}{last}"
    if pattern == "first_last":
        return f"{first}_{last}"
    if pattern == "f.lastname":
        return f"{first[0]}.{last}"
    if pattern == "last.first":
        return f"{last}.{first}"
    return None


def _base_output_columns() -> List[str]:
    return [
        "first_name",
        "last_name",
        "full_name",
        "contractor_name_normalized",
        "contractor_domain",
        "email",
        "email_pattern",
        "pattern_rank",
        "pattern_confidence",
        "title",
        "title_confirmed",
        "found_email",
        "source_url",
        "is_role_inbox",
    ]


def generate_email_candidates_for_people(
    discovered_people_df: pd.DataFrame, max_patterns_per_person: int = DEFAULT_PATTERNS_PER_PERSON
) -> pd.DataFrame:
    """
    Generate candidate emails for discovered people rows.

    Requirements handled:
    - Defaults to top conservative patterns; full pattern list remains available via max_patterns_per_person
    - Deduplicates generated emails
    - Skips rows with blank contractor_domain
    - Ignores middle initials in pattern generation
    """
    cols = _base_output_columns()
    if discovered_people_df.empty:
        return pd.DataFrame(columns=cols)

    max_patterns = max(1, min(int(max_patterns_per_person), MAX_PATTERNS_PER_PERSON))
    rows: List[Dict[str, object]] = []
    seen_emails: set[str] = set()
    seen_people_keys: set[tuple[str, str, str, str]] = set()

    for _, person in discovered_people_df.iterrows():
        domain = _normalize_domain(str(person.get("contractor_domain", "")))
        if not domain:
            continue

        first, last, full_name = _extract_name_parts(person)
        if not (first and last):
            continue

        company = normalize_text(person.get("contractor_name_normalized", "")).lower()
        person_key = (company, domain, first, last)
        if person_key in seen_people_keys:
            continue
        seen_people_keys.add(person_key)
        title = normalize_text(person.get("title", ""))
        title_confirmed = bool(person.get("title_confirmed", False))
        found_email = normalize_text(person.get("found_email", "")).strip().lower()
        source_url = normalize_text(person.get("source_url", ""))
        is_role_inbox = bool(person.get("is_role_inbox", False))

        emitted_for_person = 0
        pattern_specs = _default_pattern_specs_for_person(company, domain)
        for rank, (pattern, confidence) in enumerate(pattern_specs, start=1):
            if emitted_for_person >= max_patterns:
                break
            local_part = _build_local_part(pattern, first, last)
            if not local_part:
                continue
            email = f"{local_part}@{domain}"
            if email in seen_emails:
                continue
            seen_emails.add(email)
            emitted_for_person += 1

            rows.append(
                {
                    "first_name": first.title(),
                    "last_name": last.title(),
                    "full_name": full_name,
                    "contractor_name_normalized": company,
                    "contractor_domain": domain,
                    "email": email,
                    "email_pattern": pattern,
                    "pattern_rank": rank,
                    "pattern_confidence": confidence,
                    "title": title,
                    "title_confirmed": title_confirmed,
                    "found_email": found_email,
                    "source_url": source_url,
                    "is_role_inbox": is_role_inbox,
                }
            )

    out = pd.DataFrame(rows, columns=cols)
    if out.empty:
        return out
    out = out.drop_duplicates(subset=["email"]).reset_index(drop=True)
    return out


def generate_people_and_role_candidates(
    company_domains_df: pd.DataFrame, people_df: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Compatibility wrapper for current callers.
    Returns only individual candidates using the new generator.
    """
    candidates = generate_email_candidates_for_people(people_df)
    if candidates.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    mapped = candidates.rename(
        columns={
            "email": "email_candidate",
            "email_pattern": "generation_method",
            "first_name": "first_name_optional",
            "last_name": "last_name_optional",
            "title": "title_optional",
        }
    ).copy()
    mapped["contact_type"] = "individual"
    mapped["project_city"] = ""
    mapped["project_state"] = ""

    base_cols = [
        "contractor_name_normalized",
        "contractor_domain",
        "first_name_optional",
        "last_name_optional",
        "title_optional",
        "email_candidate",
        "contact_type",
        "generation_method",
        "project_city",
        "project_state",
    ]
    individual_df = mapped[base_cols].copy()
    role_df = pd.DataFrame(columns=base_cols)
    combined = individual_df.copy()
    return individual_df, role_df, combined
