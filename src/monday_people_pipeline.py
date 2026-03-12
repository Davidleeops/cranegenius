from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from pathlib import Path
from typing import Dict, List, Tuple
from urllib.parse import urlparse

import pandas as pd

from .domain_discovery import discover_company_domains
from .contact_page_finder import discover_contact_people
from .people_discovery import discover_people
from .people_email_generator import generate_email_candidates_for_people
from .utils import normalize_text, setup_logging
from .verify_millionverifier import verify_with_millionverifier

log = logging.getLogger("cranegenius.monday_people_pipeline")

DATA_DIR = Path("data")

INPUTS = [
    ("tier1", DATA_DIR / "monday_tier1_companies.csv"),
    ("tier2", DATA_DIR / "monday_tier2_companies.csv"),
    ("top250", DATA_DIR / "monday_combined_top_250.csv"),
    ("top500", DATA_DIR / "monday_combined_top_500.csv"),
    ("top1000", DATA_DIR / "monday_combined_top_1000.csv"),
    ("ranked", DATA_DIR / "monday_companies_ranked.csv"),
]

OUT_COMPANY_DOMAINS = DATA_DIR / "monday_company_domains.csv"
OUT_PEOPLE_FOUND = DATA_DIR / "monday_people_found.csv"

OUT_INDIVIDUAL = DATA_DIR / "monday_individual_email_candidates.csv"
OUT_ROLE = DATA_DIR / "monday_role_email_candidates.csv"
OUT_ALL = DATA_DIR / "monday_all_email_candidates.csv"

OUT_VALID = DATA_DIR / "monday_verified_valid.csv"
OUT_CATCHALL = DATA_DIR / "monday_verified_catchall.csv"
OUT_INVALID = DATA_DIR / "monday_verified_invalid.csv"

OUT_PLUS_INDIVIDUAL = DATA_DIR / "monday_plusvibes_individuals.csv"
OUT_PLUS_ROLE = DATA_DIR / "monday_plusvibes_roles.csv"
OUT_PLUS_ALL = DATA_DIR / "monday_plusvibes_combined.csv"

OUT_QA = DATA_DIR / "monday_people_pipeline_qa.json"
OUT_CONTACT_STATS = Path("runs") / "contact_generation_stats.json"
OUT_VERIFICATION_SUMMARY = Path("runs") / "verification_summary.json"
OUT_DEFERRED_VERIFICATION = DATA_DIR / "monday_verification_deferred.csv"
OUT_LOW_CONFIDENCE_DOMAINS = DATA_DIR / "monday_low_confidence_domains.csv"

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

EMAIL_LOCAL_RE = re.compile(r"^[a-z0-9](?:[a-z0-9._-]{0,62}[a-z0-9])?$")

PERSON_SOURCE_STRONG_PATH_HINTS = ("/team", "/staff", "/leadership", "/about", "/contact", "/people", "/company", "/management")
PERSON_SOURCE_WEAK_PATH_HINTS = ("", "/", "/home", "/index", "/index.html")


def _input_to_standard(df: pd.DataFrame, tier: str) -> pd.DataFrame:
    """Normalize heterogeneous monday company inputs into a standard company schema."""
    company_col = next((c for c in ["contractor_name_normalized", "company_name", "company"] if c in df.columns), "")
    if not company_col:
        return pd.DataFrame()
    domain_col = next((c for c in ["contractor_domain", "domain"] if c in df.columns), "")
    desc_col = next((c for c in ["best_project_description", "description_raw", "project_description"] if c in df.columns), "")
    city_col = next((c for c in ["project_city", "city"] if c in df.columns), "")
    state_col = next((c for c in ["project_state", "state"] if c in df.columns), "")

    out = pd.DataFrame(
        {
            "contractor_name_normalized": df[company_col].fillna("").astype(str).str.lower().str.strip(),
            "contractor_domain": df[domain_col].fillna("").astype(str) if domain_col else "",
            "project_city": df[city_col].fillna("").astype(str) if city_col else "",
            "project_state": df[state_col].fillna("").astype(str) if state_col else "",
            "best_project_description": df[desc_col].fillna("").astype(str) if desc_col else "",
            "source_rank_tier": tier,
        }
    )
    out = out[out["contractor_name_normalized"] != ""].copy()
    return out


def _format_checked_paths(paths: List[Path]) -> str:
    return "\n".join([f"- {p.resolve()}" for p in paths])


def _load_inputs(input_file: str = "") -> pd.DataFrame:
    """Load company inputs from explicit local file or monday defaults with local fallback."""
    rows: List[pd.DataFrame] = []
    checked_paths: List[Path] = []

    if input_file:
        local_path = Path(input_file).expanduser()
        if not local_path.is_absolute():
            local_path = Path.cwd() / local_path
        checked_paths.append(local_path)
        if not local_path.exists():
            raise FileNotFoundError(
                "Input file not found. Checked:\n" + _format_checked_paths(checked_paths)
            )
        df = pd.read_csv(local_path)
        std = _input_to_standard(df, tier="local_input")
        if std.empty:
            raise ValueError(
                "Input file exists but could not be mapped to required company schema. "
                "Expected one of columns: contractor_name_normalized/company_name/company. "
                f"File: {local_path}"
            )
        return std.drop_duplicates(subset=["contractor_name_normalized"], keep="first")

    for tier, path in INPUTS:
        checked_paths.append(path)
        if not path.exists():
            continue
        df = pd.read_csv(path)
        std = _input_to_standard(df, tier)
        if not std.empty:
            rows.append(std)

    if not rows:
        local_default = Path.cwd() / "companies_priority_200.csv"
        checked_paths.append(local_default)
        if local_default.exists():
            df = pd.read_csv(local_default)
            std = _input_to_standard(df, tier="local_default")
            if not std.empty:
                return std.drop_duplicates(subset=["contractor_name_normalized"], keep="first")

        raise FileNotFoundError(
            "No input company files found. Checked:\n" + _format_checked_paths(checked_paths)
        )

    combined = pd.concat(rows, ignore_index=True)
    tier_rank = {"tier1": 1, "tier2": 2, "top250": 3, "top500": 4, "top1000": 5, "ranked": 6}
    combined["tier_rank"] = combined["source_rank_tier"].map(lambda x: tier_rank.get(normalize_text(x), 9))
    combined = combined.sort_values(by=["contractor_name_normalized", "tier_rank"]).drop_duplicates(
        subset=["contractor_name_normalized"], keep="first"
    )
    return combined.drop(columns=["tier_rank"])


def _apply_clean_company_names(domains_df: pd.DataFrame) -> pd.DataFrame:
    out = domains_df.copy()
    if "cleaned_company_name" not in out.columns or "contractor_name_normalized" not in out.columns:
        return out
    cleaned = out["cleaned_company_name"].fillna("").astype(str).str.lower().str.strip()
    raw = out["contractor_name_normalized"].fillna("").astype(str).str.lower().str.strip()
    out["contractor_name_normalized"] = cleaned.where(cleaned != "", raw)
    return out


def _to_plusvibes(candidates_df: pd.DataFrame) -> pd.DataFrame:
    """Convert candidate rows into PlusVibes-compatible upload format."""
    if candidates_df.empty:
        return pd.DataFrame(columns=["to_email", "subject", "body", "company", "project_address", "city", "state", "tier"])
    out = candidates_df.copy()
    if "email_candidate" not in out.columns and "email" in out.columns:
        out["email_candidate"] = out["email"]
    if "project_city" not in out.columns:
        out["project_city"] = ""
    if "project_state" not in out.columns:
        out["project_state"] = ""
    if "contact_type" not in out.columns:
        out["contact_type"] = out.get("is_role_inbox", False).map(lambda v: "role" if bool(v) else "individual")
    out["to_email"] = out["email_candidate"]
    out["company"] = out["contractor_name_normalized"].fillna("").astype(str).str.title()
    out["city"] = out["project_city"]
    out["state"] = out["project_state"]
    out["project_address"] = ""
    out["subject"] = out.apply(
        lambda r: f"{normalize_text(r.get('company'))} — project support and lift planning",
        axis=1,
    )
    out["body"] = out.apply(
        lambda r: (
            f"Hi {normalize_text(r.get('company'))} team,\n\n"
            f"We support {normalize_text(r.get('city'))}, {normalize_text(r.get('state'))} project teams with crane and lift planning.\n\n"
            "Open to a quick call this week?"
        ),
        axis=1,
    )
    out["tier"] = out["contact_type"].map(lambda t: "individual" if t == "individual" else "role")
    return out[["to_email", "subject", "body", "company", "project_address", "city", "state", "tier"]].drop_duplicates(
        subset=["to_email"]
    )


def _is_disposable_domain(domain: str) -> bool:
    clean = normalize_text(domain).lower().strip(".")
    if not clean:
        return False
    return any(clean == d or clean.endswith(f".{d}") for d in DISPOSABLE_DOMAINS)


def _is_valid_local_part(local: str) -> bool:
    value = normalize_text(local).lower()
    if not value:
        return False
    if ".." in value:
        return False
    if not EMAIL_LOCAL_RE.match(value):
        return False
    return True


def _has_single_at_sign(email: str) -> bool:
    value = normalize_text(email)
    return value.count("@") == 1 and not value.startswith("@") and not value.endswith("@")




def _has_strong_person_evidence(row: pd.Series) -> bool:
    """Require concrete person evidence before generating or verifying guesses."""
    title_confirmed = bool(row.get("title_confirmed", False))
    found_email = normalize_text(row.get("found_email", "")).strip().lower()
    context_fields = (
        "strong_person_context_signal",
        "person_context_signal",
        "person_context_strong",
        "has_person_context",
    )
    strong_context = any(bool(row.get(field, False)) for field in context_fields)
    return bool(title_confirmed or found_email or strong_context)


def _meets_verification_confidence_gate(row: pd.Series) -> bool:
    """Conservative verification gate to avoid spending credits on weak guesses."""
    confidence = float(row.get("pattern_confidence", 0.0) or 0.0)
    if not _has_strong_person_evidence(row):
        return False
    title_confirmed = bool(row.get("title_confirmed", False))
    found_email = bool(normalize_text(row.get("found_email", "")).strip())
    source_url = normalize_text(row.get("source_url", "")).strip()
    has_source = bool(source_url)

    # Strongly sourced records can pass with slightly lower pattern confidence.
    if title_confirmed and has_source:
        return confidence >= 0.86
    if title_confirmed or found_email:
        return confidence >= 0.90
    return confidence >= 0.94


def _person_source_confidence(row: pd.Series) -> str:
    first = normalize_text(row.get("first_name", "")).strip()
    last = normalize_text(row.get("last_name", "")).strip()
    if not first or not last:
        return "low"

    source_url = normalize_text(row.get("source_url", "")).strip().lower()
    contractor_domain = normalize_text(row.get("contractor_domain", "")).strip().lower()
    title_confirmed = bool(row.get("title_confirmed", False))
    found_email = normalize_text(row.get("found_email", "")).strip().lower()

    parsed = urlparse(source_url) if source_url else None
    source_host = (parsed.netloc or "").lower().replace("www.", "") if parsed else ""
    source_path = normalize_text(parsed.path if parsed else "").lower().strip()
    host_matches = bool(contractor_domain and source_host and (source_host == contractor_domain or source_host.endswith(f".{contractor_domain}")))

    strong_path = any(hint in source_path for hint in PERSON_SOURCE_STRONG_PATH_HINTS)
    weak_path = source_path in PERSON_SOURCE_WEAK_PATH_HINTS

    if not _has_strong_person_evidence(row):
        return "low"

    score = 0
    if title_confirmed:
        score += 2
    if host_matches:
        score += 1
    if strong_path:
        score += 1
    if found_email:
        score += 2
    if source_host and not host_matches:
        score -= 1
    if weak_path and not (title_confirmed or found_email):
        score -= 1

    if score >= 4:
        return "high"
    if score >= 2:
        return "medium"
    return "low"


def _filter_people_for_personal_generation(people_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, int]]:
    if people_df.empty:
        return people_df.copy(), people_df.copy(), {"total_people_rows": 0, "deferred_low_person_source": 0, "eligible_people_rows": 0}

    work = people_df.copy()
    work["person_source_confidence"] = work.apply(_person_source_confidence, axis=1)
    work["has_strong_person_evidence"] = work.apply(_has_strong_person_evidence, axis=1).fillna(False).astype(bool)
    eligible_mask = work["person_source_confidence"].isin(["high", "medium"]) & work["has_strong_person_evidence"]
    eligible = work[eligible_mask].copy()
    deferred = work[~eligible_mask].copy()

    return eligible, deferred, {
        "total_people_rows": int(len(work)),
        "deferred_low_person_source": int(len(deferred)),
        "eligible_people_rows": int(len(eligible)),
    }


def _prepare_candidates_for_verification(all_candidates_df: pd.DataFrame, domains_df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, int]]:
    """
    Apply pre-verification filtering and return verifier input rows.
    Keeps core people-email metadata for downstream joins/auditing.
    """
    required_cols = [
        "email",
        "contractor_domain",
        "first_name",
        "last_name",
        "email_pattern",
        "pattern_confidence",
        "title",
        "title_confirmed",
        "found_email",
        "source_url",
        "is_role_inbox",
    ]
    if all_candidates_df.empty:
        return pd.DataFrame(columns=required_cols + ["email_candidate"]), {"total_raw_candidates": 0, "total_sent_to_verifier": 0}

    work = all_candidates_df.copy()
    if "email" not in work.columns and "email_candidate" in work.columns:
        work["email"] = work["email_candidate"]
    if "email_pattern" not in work.columns and "generation_method" in work.columns:
        work["email_pattern"] = work["generation_method"]
    if "first_name" not in work.columns and "first_name_optional" in work.columns:
        work["first_name"] = work["first_name_optional"]
    if "last_name" not in work.columns and "last_name_optional" in work.columns:
        work["last_name"] = work["last_name_optional"]
    if "title" not in work.columns and "title_optional" in work.columns:
        work["title"] = work["title_optional"]
    if "pattern_confidence" not in work.columns:
        work["pattern_confidence"] = 0.0
    if "pattern_rank" not in work.columns:
        work["pattern_rank"] = 999
    if "title_confirmed" not in work.columns:
        work["title_confirmed"] = False
    if "found_email" not in work.columns:
        work["found_email"] = ""
    if "source_url" not in work.columns:
        work["source_url"] = ""
    if "is_role_inbox" not in work.columns:
        work["is_role_inbox"] = False

    for c in required_cols:
        if c not in work.columns:
            work[c] = ""

    # Attach domain validation flags from domain-discovery output.
    base_cols = ["contractor_name_normalized", "contractor_domain"]
    optional_cols = ["domain_valid", "mx_valid", "domain_confidence"]
    available_cols = base_cols + [c for c in optional_cols if c in domains_df.columns]
    domain_flags = domains_df[available_cols].copy()
    if "domain_valid" not in domain_flags.columns:
        domain_flags["domain_valid"] = False
    if "mx_valid" not in domain_flags.columns:
        domain_flags["mx_valid"] = False
    if "domain_confidence" not in domain_flags.columns:
        domain_flags["domain_confidence"] = "medium"
    domain_flags["contractor_name_normalized"] = domain_flags["contractor_name_normalized"].fillna("").astype(str).str.lower().str.strip()
    domain_flags["contractor_domain"] = domain_flags["contractor_domain"].fillna("").astype(str).str.lower().str.strip()
    work["contractor_name_normalized"] = work.get("contractor_name_normalized", "").fillna("").astype(str).str.lower().str.strip()
    work["contractor_domain"] = work["contractor_domain"].fillna("").astype(str).str.lower().str.strip()
    work = work.merge(
        domain_flags,
        on=["contractor_name_normalized", "contractor_domain"],
        how="left",
        suffixes=("", "_domain"),
    )
    work["domain_valid"] = work.get("domain_valid", False).fillna(False).astype(bool)
    work["mx_valid"] = work.get("mx_valid", False).fillna(False).astype(bool)
    work["domain_confidence"] = work.get("domain_confidence", "low").fillna("low").astype(str).str.lower()

    total_raw = int(len(work))

    valid_domain_mask = work["domain_valid"].astype(bool)
    filtered_invalid_domain = int((~valid_domain_mask).sum())
    work = work[valid_domain_mask].copy()

    mx_valid_mask = work["mx_valid"].astype(bool)
    filtered_no_mx = int((~mx_valid_mask).sum())
    work = work[mx_valid_mask].copy()

    confidence_domain_mask = work["domain_confidence"].isin(["high", "medium"])
    filtered_low_domain_confidence = int((~confidence_domain_mask).sum())
    work = work.loc[confidence_domain_mask].copy()

    email_series = work["email"].fillna("").astype(str).str.strip().str.lower()
    local_series = email_series.str.split("@").str[0].fillna("")
    malformed_mask = ((~email_series.map(_has_single_at_sign)) | (~local_series.map(_is_valid_local_part))).fillna(True).astype(bool)
    filtered_malformed = int(malformed_mask.sum())
    work = work.loc[~malformed_mask].copy()

    disposable_mask = work["contractor_domain"].map(_is_disposable_domain).fillna(False).astype(bool)
    filtered_disposable = int(disposable_mask.sum())
    work = work.loc[~disposable_mask].copy()

    role_inbox_mask = work["is_role_inbox"].fillna(False).astype(bool)
    filtered_role_inbox = int(role_inbox_mask.sum())
    work = work.loc[~role_inbox_mask].copy()

    if work.empty:
        filtered_low_confidence = 0
    else:
        confidence_mask = work.apply(_meets_verification_confidence_gate, axis=1).fillna(False).astype(bool)
        filtered_low_confidence = int((~confidence_mask).sum())
        work = work.loc[confidence_mask].copy()

    before_dedupe = len(work)
    work = work.drop_duplicates(subset=["email"])
    removed_by_dedupe = int(before_dedupe - len(work))
    sent_to_verifier = int(len(work))

    log.info(
        "Pre-verification filter counts | total_raw_candidates=%d filtered_invalid_domain=%d filtered_no_mx=%d filtered_low_domain_confidence=%d filtered_malformed_email=%d filtered_disposable=%d filtered_role_inbox=%d filtered_low_confidence=%d removed_by_dedupe=%d total_sent_to_verifier=%d",
        total_raw,
        filtered_invalid_domain,
        filtered_no_mx,
        filtered_low_domain_confidence,
        filtered_malformed,
        filtered_disposable,
        filtered_role_inbox,
        filtered_low_confidence,
        removed_by_dedupe,
        sent_to_verifier,
    )

    out = work[required_cols].copy()
    out["email_candidate"] = out["email"]
    return out, {
        "total_raw_candidates": total_raw,
        "filtered_invalid_domain": filtered_invalid_domain,
        "filtered_no_mx": filtered_no_mx,
        "filtered_low_domain_confidence": filtered_low_domain_confidence,
        "filtered_malformed": filtered_malformed,
        "filtered_disposable": filtered_disposable,
        "filtered_role_inbox": filtered_role_inbox,
        "filtered_low_confidence": filtered_low_confidence,
        "removed_by_dedupe": removed_by_dedupe,
        "total_sent_to_verifier": sent_to_verifier,
    }


def _verify(all_candidates_df: pd.DataFrame, domains_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, Dict[str, object]]:
    """Verify email candidates with MillionVerifier and return explicit execution state."""
    cols = list(all_candidates_df.columns) + [
        "email_verification_status",
        "email_is_catchall",
        "email_quality_score",
        "email_verification_provider",
    ]

    def _meta(
        *,
        attempted: bool,
        skipped: bool,
        reason: str,
        total_candidates_generated: int,
        total_sent_to_verifier: int,
        total_verified_rows_returned: int,
        valid_count: int,
        catchall_count: int,
        invalid_count: int,
    ) -> Dict[str, object]:
        return {
            "verification_attempted": attempted,
            "verification_skipped": skipped,
            "skip_reason": reason,
            "total_candidates_generated": total_candidates_generated,
            "total_sent_to_verifier": total_sent_to_verifier,
            "total_verified_rows_returned": total_verified_rows_returned,
            "valid_count": valid_count,
            "catchall_count": catchall_count,
            "invalid_count": invalid_count,
        }

    if all_candidates_df.empty:
        log.info("Verification skipped: no candidates generated.")
        empty = pd.DataFrame(columns=cols)
        return empty, empty, empty, _meta(
            attempted=False,
            skipped=True,
            reason="no_candidates",
            total_candidates_generated=0,
            total_sent_to_verifier=0,
            total_verified_rows_returned=0,
            valid_count=0,
            catchall_count=0,
            invalid_count=0,
        )

    verifier_input, filter_counts = _prepare_candidates_for_verification(all_candidates_df, domains_df)
    total_candidates_generated = int(len(all_candidates_df))
    total_sent_to_verifier = int(filter_counts.get("total_sent_to_verifier", 0))

    if verifier_input.empty:
        log.info("Verification skipped: no verifier input after gating.")
        empty = pd.DataFrame(columns=cols)
        return empty, empty, empty, _meta(
            attempted=False,
            skipped=True,
            reason="no_verifier_input",
            total_candidates_generated=total_candidates_generated,
            total_sent_to_verifier=total_sent_to_verifier,
            total_verified_rows_returned=0,
            valid_count=0,
            catchall_count=0,
            invalid_count=0,
        )

    api_key = os.environ.get("MILLIONVERIFIER_API_KEY", "").strip()
    if not api_key:
        log.info("Verification skipped: MILLIONVERIFIER_API_KEY missing.")
        empty = pd.DataFrame(columns=cols)
        return empty, empty, empty, _meta(
            attempted=False,
            skipped=True,
            reason="missing_api_key",
            total_candidates_generated=total_candidates_generated,
            total_sent_to_verifier=total_sent_to_verifier,
            total_verified_rows_returned=0,
            valid_count=0,
            catchall_count=0,
            invalid_count=0,
        )

    verified = verify_with_millionverifier(verifier_input)
    merged = all_candidates_df.copy()
    if "email" not in merged.columns and "email_candidate" in merged.columns:
        merged["email"] = merged["email_candidate"]
    merged["email"] = merged["email"].fillna("").astype(str).str.lower()
    verified["email"] = verified["email"].fillna("").astype(str).str.lower()
    merged = merged.merge(verified, on="email", how="left")
    valid = merged[merged["email_verification_status"] == "valid"].copy()
    catchall = merged[merged["email_verification_status"] == "catchall"].copy()
    invalid = merged[~merged["email_verification_status"].isin(["valid", "catchall"])].copy()

    log.info(
        "Verification executed successfully | sent_to_verifier=%d returned_rows=%d valid=%d catchall=%d invalid=%d",
        total_sent_to_verifier,
        int(len(verified)),
        int(len(valid)),
        int(len(catchall)),
        int(len(invalid)),
    )

    return valid, catchall, invalid, _meta(
        attempted=True,
        skipped=False,
        reason="",
        total_candidates_generated=total_candidates_generated,
        total_sent_to_verifier=total_sent_to_verifier,
        total_verified_rows_returned=int(len(verified)),
        valid_count=int(len(valid)),
        catchall_count=int(len(catchall)),
        invalid_count=int(len(invalid)),
    )


def run_pipeline(max_companies: int = 0, input_file: str = "") -> Dict[str, float]:
    """Execute the monday people-intelligence pipeline and write all outputs."""
    input_df = _load_inputs(input_file=input_file)
    if max_companies and max_companies > 0:
        input_df = input_df.head(int(max_companies)).copy()
    total_input_companies = int(input_df["contractor_name_normalized"].nunique())

    domains_df = discover_company_domains(input_df)
    domains_df = _apply_clean_company_names(domains_df)
    domains_df.to_csv(OUT_COMPANY_DOMAINS, index=False)
    companies_with_domains = int((domains_df["contractor_domain"].fillna("").astype(str).str.strip() != "").sum())

    people_df = discover_people(domains_df, max_people_per_company=3)

    # Contact-page fallback for companies that had a valid domain but no people rows.
    people_companies = set(people_df["contractor_name_normalized"].astype(str).str.lower().str.strip()) if not people_df.empty else set()
    unresolved_people_df = domains_df[
        (domains_df["domain_valid"].astype(bool))
        & (~domains_df["contractor_name_normalized"].astype(str).str.lower().str.strip().isin(people_companies))
    ].copy()
    contact_people_df = discover_contact_people(unresolved_people_df)

    combined_people_df = pd.concat([people_df, contact_people_df], ignore_index=True, sort=False)
    if not combined_people_df.empty:
        combined_people_df = combined_people_df.drop_duplicates(
            subset=["contractor_name_normalized", "contractor_domain", "first_name", "last_name"], keep="first"
        )

    combined_people_df.to_csv(OUT_PEOPLE_FOUND, index=False)
    companies_with_people = int(combined_people_df["contractor_name_normalized"].nunique()) if not combined_people_df.empty else 0
    avg_people_per_company = round(float(len(combined_people_df) / companies_with_people), 3) if companies_with_people else 0.0

    if "contractor_domain" in combined_people_df.columns:
        combined_people_df["contractor_domain"] = combined_people_df["contractor_domain"].fillna("").astype(str).str.lower().str.strip()

    people_domains_conf_cols = ["contractor_domain"] + (["domain_confidence"] if "domain_confidence" in domains_df.columns else [])
    people_domains_conf = domains_df[people_domains_conf_cols].copy()
    if "domain_confidence" not in people_domains_conf.columns:
        people_domains_conf["domain_confidence"] = "medium"
    people_domains_conf["contractor_domain"] = people_domains_conf["contractor_domain"].fillna("").astype(str).str.lower().str.strip()
    people_domains_conf = people_domains_conf.drop_duplicates(subset=["contractor_domain"], keep="first")
    people_with_conf_df = combined_people_df.merge(people_domains_conf, on=["contractor_domain"], how="left")
    people_with_conf_df["domain_confidence"] = people_with_conf_df.get("domain_confidence", "low").fillna("low").astype(str).str.lower()

    domain_eligible_people_df = people_with_conf_df[people_with_conf_df["domain_confidence"].isin(["high", "medium"])].copy()
    domain_deferred_people_df = people_with_conf_df[~people_with_conf_df["domain_confidence"].isin(["high", "medium"])].copy()

    eligible_people_df, low_source_people_df, people_source_counts = _filter_people_for_personal_generation(domain_eligible_people_df)

    low_conf_people_df = pd.concat([domain_deferred_people_df, low_source_people_df], ignore_index=True, sort=False)
    if not low_conf_people_df.empty:
        low_conf_people_df = low_conf_people_df.drop_duplicates(subset=["contractor_name_normalized", "contractor_domain", "first_name", "last_name", "source_url"], keep="first")
    low_conf_people_df.to_csv(OUT_LOW_CONFIDENCE_DOMAINS, index=False)

    log.info(
        "People source gating | total_people_rows=%d eligible_people_rows=%d deferred_low_person_source=%d deferred_low_domain=%d",
        int(people_source_counts.get("total_people_rows", 0)),
        int(people_source_counts.get("eligible_people_rows", 0)),
        int(people_source_counts.get("deferred_low_person_source", 0)),
        int(len(domain_deferred_people_df)),
    )

    all_df = generate_email_candidates_for_people(eligible_people_df)
    verifier_input_df, filter_counts = _prepare_candidates_for_verification(all_df, domains_df)
    deferred_df = all_df.copy()
    if not deferred_df.empty and "email" in verifier_input_df.columns:
        keep = set(verifier_input_df["email"].fillna("").astype(str).str.lower())
        deferred_df = deferred_df[~deferred_df["email"].fillna("").astype(str).str.lower().isin(keep)].copy()
    deferred_df.to_csv(OUT_DEFERRED_VERIFICATION, index=False)
    log.info(
        "Candidate generation summary | candidates_generated=%d candidates_sent_to_verifier=%d",
        int(len(all_df)),
        int(filter_counts.get("total_sent_to_verifier", 0)),
    )
    individual_df = all_df[~all_df["is_role_inbox"].astype(bool)].copy() if not all_df.empty else pd.DataFrame()
    role_df = all_df[all_df["is_role_inbox"].astype(bool)].copy() if not all_df.empty else pd.DataFrame()
    individual_df.to_csv(OUT_INDIVIDUAL, index=False)
    role_df.to_csv(OUT_ROLE, index=False)
    all_df.to_csv(OUT_ALL, index=False)

    verified_valid, verified_catchall, verified_invalid, verification_meta = _verify(all_df, domains_df)
    verified_valid.to_csv(OUT_VALID, index=False)
    verified_catchall.to_csv(OUT_CATCHALL, index=False)
    verified_invalid.to_csv(OUT_INVALID, index=False)

    plus_ind = _to_plusvibes(individual_df)
    plus_role = _to_plusvibes(role_df)
    plus_all = _to_plusvibes(all_df)
    plus_ind.to_csv(OUT_PLUS_INDIVIDUAL, index=False)
    plus_role.to_csv(OUT_PLUS_ROLE, index=False)
    plus_all.to_csv(OUT_PLUS_ALL, index=False)

    qa = {
        "total_input_companies": total_input_companies,
        "companies_with_domains": companies_with_domains,
        "people_found_count": int(len(combined_people_df)),
        "companies_with_people": companies_with_people,
        "avg_people_per_company": avg_people_per_company,
        "individual_candidates_generated": int(len(individual_df)),
        "role_candidates_generated": int(len(role_df)),
        "total_candidates_generated": int(len(all_df)),
        "verified_valid_count": int(len(verified_valid)),
        "verified_catchall_count": int(len(verified_catchall)),
        "verified_invalid_count": int(len(verified_invalid)),
        "avg_candidates_per_company": round(float(len(all_df) / total_input_companies), 3) if total_input_companies else 0.0,
    }
    OUT_QA.write_text(json.dumps(qa, indent=2), encoding="utf-8")

    OUT_CONTACT_STATS.parent.mkdir(parents=True, exist_ok=True)
    stats_payload = {
        "companies_processed": total_input_companies,
        "domains_found": companies_with_domains,
        "people_rows_total": int(people_source_counts.get("total_people_rows", 0)),
        "people_rows_eligible": int(people_source_counts.get("eligible_people_rows", 0)),
        "people_rows_deferred_low_person_source": int(people_source_counts.get("deferred_low_person_source", 0)),
        "people_rows_deferred_low_domain": int(len(domain_deferred_people_df)),
        "emails_generated": int(len(all_df)),
        "emails_filtered": int(filter_counts.get("total_raw_candidates", 0) - filter_counts.get("total_sent_to_verifier", 0)),
        "emails_ready_for_verification": int(filter_counts.get("total_sent_to_verifier", 0)),
    }
    OUT_CONTACT_STATS.write_text(json.dumps(stats_payload, indent=2), encoding="utf-8")

    verification_payload = {
        "verification_attempted": bool(verification_meta.get("verification_attempted", False)),
        "verification_skipped": bool(verification_meta.get("verification_skipped", True)),
        "skip_reason": normalize_text(verification_meta.get("skip_reason", "")),
        "total_candidates_generated": int(verification_meta.get("total_candidates_generated", len(all_df))),
        "total_sent_to_verifier": int(verification_meta.get("total_sent_to_verifier", filter_counts.get("total_sent_to_verifier", 0))),
        "total_verified_rows_returned": int(verification_meta.get("total_verified_rows_returned", 0)),
        "valid_count": int(verification_meta.get("valid_count", len(verified_valid))),
        "catchall_count": int(verification_meta.get("catchall_count", len(verified_catchall))),
        "invalid_count": int(verification_meta.get("invalid_count", len(verified_invalid))),
        "valid_output": str(OUT_VALID),
        "catchall_output": str(OUT_CATCHALL),
        "invalid_output": str(OUT_INVALID),
    }
    OUT_VERIFICATION_SUMMARY.parent.mkdir(parents=True, exist_ok=True)
    OUT_VERIFICATION_SUMMARY.write_text(json.dumps(verification_payload, indent=2), encoding="utf-8")

    return qa


def run_qa_check(df: pd.DataFrame) -> Dict[str, object]:
    """Run outbound quality thresholds and return go/no-go report."""
    out = df.copy()
    if "domain" not in out.columns:
        out["domain"] = ""
    if "email" not in out.columns and "email_candidate" in out.columns:
        out["email"] = out["email_candidate"]
    if "email" not in out.columns:
        out["email"] = ""
    if "is_role_inbox" not in out.columns:
        out["is_role_inbox"] = False
    if "title_confirmed" not in out.columns:
        out["title_confirmed"] = False
    if "verification_status" not in out.columns:
        out["verification_status"] = out.get("email_verification_status", "")

    total_contacts = int(len(out))
    domain_non_null = int(out["domain"].fillna("").astype(str).str.strip().ne("").sum())
    domain_coverage_pct = round((domain_non_null / total_contacts * 100), 2) if total_contacts else 0.0

    # company proxy uses domain first, then email domain.
    company_key = out["domain"].fillna("").astype(str).str.strip().str.lower()
    missing_domain = company_key.eq("")
    email_domain = out["email"].fillna("").astype(str).str.lower().str.extract(r"@([a-z0-9\.-]+\.[a-z]{2,})")[0].fillna("")
    company_key.loc[missing_domain] = email_domain.loc[missing_domain]
    company_key = company_key.replace("", "unknown")
    companies_total = int(company_key.nunique())

    named_individual = (~out["is_role_inbox"].astype(bool))
    companies_with_people = int(company_key[named_individual].nunique()) if total_contacts else 0
    people_found_rate = round((companies_with_people / companies_total * 100), 2) if companies_total else 0.0

    verification_series = out["verification_status"].fillna("").astype(str).str.lower()
    pass_count = int(verification_series.isin(["deliverable", "risky"]).sum())
    verification_pass_rate = round((pass_count / total_contacts * 100), 2) if total_contacts else 0.0

    role_count = int(out["is_role_inbox"].astype(bool).sum())
    role_inbox_pct = round((role_count / total_contacts * 100), 2) if total_contacts else 0.0

    stop_reasons: List[str] = []
    if domain_coverage_pct < 60:
        stop_reasons.append("Domain coverage is below 60%.")
    if people_found_rate < 35:
        stop_reasons.append("People found rate is below 35%.")
    if verification_pass_rate < 65:
        stop_reasons.append("Verification pass rate is below 65%.")
    if role_inbox_pct > 30:
        stop_reasons.append("Role inbox percentage is above 30%.")
    if total_contacts < 100:
        stop_reasons.append("Total contacts is below 100.")

    return {
        "total_contacts": total_contacts,
        "domain_coverage_pct": domain_coverage_pct,
        "people_found_rate": people_found_rate,
        "verification_pass_rate": verification_pass_rate,
        "role_inbox_pct": role_inbox_pct,
        "go": len(stop_reasons) == 0,
        "stop_reasons": stop_reasons,
    }


def _run_qa_check_cli() -> int:
    """Load verified_contacts.csv, run QA thresholds, print JSON, and return process exit code."""
    path = DATA_DIR / "verified_contacts.csv"
    if not path.exists():
        report = run_qa_check(pd.DataFrame(columns=["domain", "email", "is_role_inbox", "title_confirmed", "verification_status"]))
        print(json.dumps(report, indent=2))
        return 1
    df = pd.read_csv(path)
    report = run_qa_check(df)
    print(json.dumps(report, indent=2))
    return 0 if report.get("go") else 1


def main() -> None:
    """CLI entry: run pipeline (default) or QA threshold checker mode."""
    parser = argparse.ArgumentParser(description="CraneGenius monday people pipeline")
    parser.add_argument(
        "--mode",
        choices=["run", "qa-check"],
        default="run",
        help="run: execute people pipeline; qa-check: evaluate verified_contacts thresholds",
    )
    parser.add_argument(
        "--max-companies",
        type=int,
        default=0,
        help="Optional cap for small-slice runs (0 means all companies).",
    )
    parser.add_argument(
        "--input-file",
        type=str,
        default="",
        help="Optional local CSV file path for pipeline input (overrides monday defaults).",
    )
    args = parser.parse_args()

    setup_logging()
    if args.mode == "qa-check":
        raise SystemExit(_run_qa_check_cli())

    qa = run_pipeline(max_companies=args.max_companies, input_file=args.input_file)
    log.info("Monday people pipeline complete: %s", qa)


if __name__ == "__main__":
    sys.exit(main() or 0)
