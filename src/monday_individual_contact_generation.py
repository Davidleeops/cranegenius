from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

from .utils import normalize_text, setup_logging

log = logging.getLogger("cranegenius.monday_individual_contacts")

DATA_DIR = Path("data")
PRIMARY_INPUT = DATA_DIR / "monday_combined_top_250.csv"
FALLBACK_INPUTS = [
    DATA_DIR / "monday_top_250_companies.csv",
    DATA_DIR / "monday_companies_ranked.csv",
    DATA_DIR / "monday_campaign_companies.csv",
]

OUT_INDIVIDUAL = DATA_DIR / "monday_individual_email_candidates.csv"
OUT_ROLE = DATA_DIR / "monday_role_email_candidates.csv"
OUT_ALL = DATA_DIR / "monday_all_candidates.csv"
OUT_QA = DATA_DIR / "monday_individual_candidates_qa.json"

ROLE_BUCKETS = [
    ("project_manager", "Project Manager"),
    ("estimator", "Estimator"),
    ("preconstruction", "Preconstruction Manager"),
    ("superintendent", "Superintendent"),
]

ROLE_INBOX_PREFIX = {
    "project_manager": "projects",
    "estimator": "estimating",
    "preconstruction": "preconstruction",
    "superintendent": "operations",
}

FIRST_NAMES = [
    "Alex", "Jordan", "Taylor", "Morgan", "Riley", "Cameron", "Avery", "Casey",
    "Drew", "Emerson", "Parker", "Quinn", "Blake", "Hayden", "Reese", "Rowan",
]
LAST_NAMES = [
    "Carter", "Bennett", "Sullivan", "Hayes", "Walker", "Miller", "Parker", "Brooks",
    "Reed", "Foster", "Powell", "Griffin", "Coleman", "Perry", "Hayward", "Shaw",
]


def _pick_input_file() -> Path:
    if PRIMARY_INPUT.exists():
        return PRIMARY_INPUT
    for p in FALLBACK_INPUTS:
        if p.exists():
            log.warning("Primary input missing; using fallback: %s", p)
            return p
    raise FileNotFoundError(
        f"No input file found. Expected {PRIMARY_INPUT} or one of {[str(x) for x in FALLBACK_INPUTS]}"
    )


def _company_domain_cols(df: pd.DataFrame) -> Tuple[str, str]:
    company_candidates = ["contractor_name_normalized", "company_name", "company"]
    domain_candidates = ["contractor_domain", "domain"]
    company_col = next((c for c in company_candidates if c in df.columns), "")
    domain_col = next((c for c in domain_candidates if c in df.columns), "")
    if not company_col:
        raise ValueError("Input file must include contractor_name_normalized/company_name/company column")
    return company_col, domain_col


def _domain_primary(value: str) -> str:
    v = normalize_text(value).lower()
    if not v:
        return ""
    return v.split("|")[0].strip()


def _placeholder_name(company: str, role_bucket: str) -> Tuple[str, str]:
    key = f"{company}|{role_bucket}".encode("utf-8")
    digest = hashlib.md5(key).hexdigest()  # deterministic placeholder generation
    a = int(digest[:8], 16)
    b = int(digest[8:16], 16)
    return FIRST_NAMES[a % len(FIRST_NAMES)], LAST_NAMES[b % len(LAST_NAMES)]


def _individual_patterns(first: str, last: str, domain: str) -> List[Tuple[str, str]]:
    f = normalize_text(first).lower()
    l = normalize_text(last).lower()
    if not f or not l or not domain:
        return []
    return [
        (f"{f}.{l}@{domain}", "placeholder_pattern_first_last"),
        (f"{f}@{domain}", "placeholder_pattern_first"),
        (f"{f[0]}_{l}@{domain}", "placeholder_pattern_first_initial_last"),
    ]


def run() -> Dict[str, int]:
    src = _pick_input_file()
    df = pd.read_csv(src)
    company_col, domain_col = _company_domain_cols(df)

    base = df[[company_col] + ([domain_col] if domain_col else [])].copy()
    base["company_name"] = base[company_col].fillna("").astype(str).map(normalize_text)
    base["domain"] = base[domain_col].fillna("").astype(str).map(_domain_primary) if domain_col else ""
    base = base[base["company_name"] != ""].drop_duplicates(subset=["company_name", "domain"])
    with_domains = base[base["domain"] != ""].copy()

    individual_rows: List[Dict[str, str]] = []
    role_rows: List[Dict[str, str]] = []

    for _, row in with_domains.iterrows():
        company = row["company_name"]
        domain = row["domain"]
        for role_bucket, _role_title in ROLE_BUCKETS:
            first, last = _placeholder_name(company, role_bucket)
            for email, method in _individual_patterns(first, last, domain):
                individual_rows.append(
                    {
                        "company_name": company,
                        "domain": domain,
                        "contact_role_bucket": role_bucket,
                        "email_candidate": email,
                        "generation_method": method,
                    }
                )

            inbox = f"{ROLE_INBOX_PREFIX[role_bucket]}@{domain}"
            role_rows.append(
                {
                    "company_name": company,
                    "domain": domain,
                    "contact_role_bucket": role_bucket,
                    "email_candidate": inbox,
                    "generation_method": "role_inbox",
                }
            )

    individual_df = pd.DataFrame(individual_rows).drop_duplicates(subset=["email_candidate"])
    role_df = pd.DataFrame(role_rows).drop_duplicates(subset=["email_candidate"])
    all_df = pd.concat([individual_df, role_df], ignore_index=True).drop_duplicates(subset=["email_candidate"])

    individual_df.to_csv(OUT_INDIVIDUAL, index=False)
    role_df.to_csv(OUT_ROLE, index=False)
    all_df.to_csv(OUT_ALL, index=False)

    qa = {
        "input_file": str(src),
        "total_companies": int(base["company_name"].nunique()),
        "companies_with_domains": int(with_domains["company_name"].nunique()),
        "individual_candidates_generated": int(len(individual_df)),
        "role_candidates_generated": int(len(role_df)),
        "total_candidates": int(len(all_df)),
    }
    OUT_QA.write_text(json.dumps(qa, indent=2), encoding="utf-8")
    return qa


def main() -> None:
    setup_logging()
    qa = run()
    log.info("Monday individual contact generation complete: %s", qa)


if __name__ == "__main__":
    main()
