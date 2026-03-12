from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

from .utils import normalize_text, setup_logging
from .verify_millionverifier import verify_with_millionverifier

log = logging.getLogger("cranegenius.monday_fast_path")

DATA_DIR = Path("data")
OUTPUT_COMPANIES = DATA_DIR / "monday_campaign_companies.csv"
OUTPUT_CANDIDATES = DATA_DIR / "monday_campaign_email_candidates.csv"
OUTPUT_VERIFIED = DATA_DIR / "monday_campaign_verified.csv"
OUTPUT_PLUSVIBES = DATA_DIR / "monday_campaign_plusvibes.csv"
OUTPUT_QA = DATA_DIR / "monday_campaign_qa.json"

ROLE_ORDER = ["estimating", "bids", "projects", "operations", "info"]
MAX_CONTACTS_PER_COMPANY = 2
MIN_COST_DEFAULT = 2_000_000

RESIDENTIAL_EXCLUSIONS = [
    "single-family",
    "single family",
    "duplex",
    "townhouse",
    "townhome",
    "garage",
    "porch",
    "fence",
    "basement",
    "rehab",
    "small addition",
]

STRONG_PROJECT_TERMS = [
    "data center",
    "hospital",
    "warehouse",
    "logistics",
    "manufacturing",
    "structural steel",
    "curtain wall",
    "glazing",
    "generator",
    "switchgear",
    "rooftop hvac",
    "mixed-use",
    "mixed use",
    "mid-rise",
    "mid rise",
]

COST_M_RE = re.compile(r"(\d+(?:\.\d+)?)\s*m(?:illion)?\b", re.IGNORECASE)
COST_PLAIN_RE = re.compile(r"\$?\s*([0-9]{1,3}(?:,[0-9]{3})+|[0-9]{7,})")


def _load_base_dataset() -> pd.DataFrame:
    # Fast path: use existing scored + enriched data; fallback safely.
    if (DATA_DIR / "enriched_companies.csv").exists():
        df = pd.read_csv(DATA_DIR / "enriched_companies.csv")
        log.info("Loaded %d rows from enriched_companies.csv", len(df))
    elif (DATA_DIR / "scored_records.csv").exists():
        df = pd.read_csv(DATA_DIR / "scored_records.csv")
        log.info("Loaded %d rows from scored_records.csv", len(df))
    elif (DATA_DIR / "normalized_records.csv").exists():
        df = pd.read_csv(DATA_DIR / "normalized_records.csv")
        if "lift_probability_score" not in df.columns:
            df["lift_probability_score"] = 0
        log.info("Loaded %d rows from normalized_records.csv", len(df))
    else:
        raise FileNotFoundError("No source CSV found in data/.")

    if "contractor_domain" not in df.columns:
        df["contractor_domain"] = ""
    if "contractor_name_normalized" not in df.columns:
        df["contractor_name_normalized"] = (
            df.get("contractor_name_raw", "").fillna("").astype(str).str.lower().str.strip()
        )
    if "description_raw" not in df.columns:
        df["description_raw"] = ""
    if "record_date_iso" not in df.columns:
        df["record_date_iso"] = ""
    if "record_date" not in df.columns:
        df["record_date"] = ""
    if "project_city" not in df.columns:
        df["project_city"] = ""
    if "project_state" not in df.columns:
        df["project_state"] = ""
    if "project_address" not in df.columns:
        df["project_address"] = ""
    if "score_hits" not in df.columns:
        df["score_hits"] = ""
    if "lift_probability_score" not in df.columns:
        df["lift_probability_score"] = 0
    return df


def _seed_domains_from_file(df: pd.DataFrame) -> pd.DataFrame:
    seed_path = DATA_DIR / "company_domain_seed.csv"
    if not seed_path.exists():
        return df
    seed_df = pd.read_csv(seed_path)
    if seed_df.empty:
        return df
    seed_map = {
        normalize_text(r.get("contractor_name_normalized")).lower(): normalize_text(r.get("contractor_domain")).lower()
        for _, r in seed_df.iterrows()
        if normalize_text(r.get("contractor_name_normalized")) and normalize_text(r.get("contractor_domain"))
    }
    out = df.copy()
    out["contractor_domain"] = out["contractor_domain"].fillna("").astype(str)
    missing = out["contractor_domain"].str.strip() == ""
    out.loc[missing, "contractor_domain"] = (
        out.loc[missing, "contractor_name_normalized"]
        .fillna("")
        .astype(str)
        .str.lower()
        .map(seed_map)
        .fillna("")
    )
    return out


def _cost_from_row(row: pd.Series) -> float:
    raw = normalize_text(row.get("project_cost_optional", ""))
    if raw:
        try:
            return float(str(raw).replace("$", "").replace(",", "").strip())
        except Exception:
            pass
    text = normalize_text(row.get("description_raw", ""))
    m = COST_M_RE.search(text)
    if m:
        return float(m.group(1)) * 1_000_000
    m2 = COST_PLAIN_RE.search(text)
    if m2:
        return float(m2.group(1).replace(",", ""))
    return 0.0


def _primary_domain(value: str) -> str:
    v = normalize_text(value).lower()
    if not v:
        return ""
    return v.split("|")[0].strip()


def _timestamp(row: pd.Series) -> pd.Timestamp:
    dt = pd.to_datetime(row.get("record_date_iso", ""), errors="coerce", utc=True)
    if pd.isna(dt):
        dt = pd.to_datetime(row.get("record_date", ""), errors="coerce", utc=True)
    if pd.isna(dt):
        return pd.Timestamp(datetime(1970, 1, 1, tzinfo=timezone.utc))
    return dt


def _contains_any(text: str, terms: List[str]) -> bool:
    t = normalize_text(text).lower()
    return any(term in t for term in terms)


def _build_company_selection(df: pd.DataFrame, min_cost: int) -> Tuple[pd.DataFrame, Dict[str, int]]:
    out = df.copy()
    out["description_raw"] = out["description_raw"].fillna("").astype(str)
    out["lift_probability_score"] = pd.to_numeric(out["lift_probability_score"], errors="coerce").fillna(0)
    out["project_cost_num"] = out.apply(_cost_from_row, axis=1)
    out["primary_domain"] = out["contractor_domain"].map(_primary_domain)
    out["company_group_key"] = out["primary_domain"]
    missing = out["company_group_key"].str.strip() == ""
    out.loc[missing, "company_group_key"] = (
        out.loc[missing, "contractor_name_normalized"].fillna("").astype(str).str.lower().str.strip()
    )
    out["company_group_key"] = out["company_group_key"].replace("", "unknown")

    out["is_residential_excluded"] = out["description_raw"].map(
        lambda x: _contains_any(x, RESIDENTIAL_EXCLUSIONS)
    )
    out["is_strong_project_type"] = (
        out["description_raw"].map(lambda x: _contains_any(x, STRONG_PROJECT_TERMS))
        | out["score_hits"].fillna("").astype(str).str.lower().map(lambda x: _contains_any(x, STRONG_PROJECT_TERMS))
    )
    out["passes_cost"] = (out["project_cost_num"] >= float(min_cost)) | (
        (out["project_cost_num"] <= 0) & (out["lift_probability_score"] >= 7) & (out["is_strong_project_type"])
    )

    excluded_residential_count = int(out["is_residential_excluded"].sum())
    kept = out[(~out["is_residential_excluded"]) & (out["passes_cost"]) & (out["is_strong_project_type"])].copy()
    kept["record_ts"] = kept.apply(_timestamp, axis=1)
    kept["keyword_strength"] = kept["description_raw"].str.lower().map(
        lambda x: sum(1 for t in STRONG_PROJECT_TERMS if t in x)
    )
    kept["send_priority_score"] = (
        kept["lift_probability_score"] * 3
        + (kept["project_cost_num"] / 1_000_000.0).clip(upper=40)
        + kept["keyword_strength"] * 2
    )

    ranked = kept.sort_values(
        by=["company_group_key", "lift_probability_score", "project_cost_num", "record_ts", "send_priority_score"],
        ascending=[True, False, False, False, False],
    )
    selected = ranked.drop_duplicates(subset=["company_group_key"], keep="first").copy()
    return selected, {"excluded_residential_count": excluded_residential_count}


def _build_candidates(selected_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, r in selected_df.iterrows():
        domain = normalize_text(r.get("primary_domain", "")).lower()
        if not domain:
            continue
        count = 0
        for prefix in ROLE_ORDER:
            if count >= MAX_CONTACTS_PER_COMPANY:
                break
            rows.append(
                {
                    "company_group_key": r.get("company_group_key", ""),
                    "contractor_domain": domain,
                    "contractor_name_normalized": r.get("contractor_name_normalized", ""),
                    "email_candidate": f"{prefix}@{domain}",
                    "contact_role_bucket": "role_inbox",
                    "generation_method": "monday_fast_path",
                    "lift_probability_score": r.get("lift_probability_score", 0),
                    "project_city": r.get("project_city", ""),
                    "project_state": r.get("project_state", ""),
                    "project_address": r.get("project_address", ""),
                }
            )
            count += 1
    return pd.DataFrame(rows)


def _to_plusvibes(df: pd.DataFrame, selected_by_company: pd.DataFrame, tier: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["to_email", "subject", "body", "company", "project_address", "city", "state", "tier"])
    project_cols = selected_by_company[
        [
            "company_group_key",
            "contractor_name_normalized",
            "description_raw",
            "project_address",
            "project_city",
            "project_state",
        ]
    ].rename(
        columns={
            "contractor_name_normalized": "company_name",
            "description_raw": "project_description",
            "project_address": "project_address_selected",
            "project_city": "project_city_selected",
            "project_state": "project_state_selected",
        }
    )
    joined = df.merge(
        project_cols,
        on="company_group_key",
        how="left",
    )
    joined["to_email"] = joined["email_candidate"]
    joined["company"] = joined["company_name"].fillna(joined.get("contractor_name_normalized", "")).fillna("")
    joined["city"] = joined["project_city_selected"]
    joined["state"] = joined["project_state_selected"]
    joined["subject"] = joined.apply(
        lambda r: f"{normalize_text(r.get('company')).title()} — lift planning and crane support",
        axis=1,
    )
    joined["body"] = joined.apply(
        lambda r: (
            f"Hi team,\n\nSaw recent work tied to {normalize_text(r.get('project_description'))[:120]} in "
            f"{normalize_text(r.get('city'))}, {normalize_text(r.get('state'))}. "
            "We help contractors secure crane and lift coverage quickly.\n\n"
            "Worth a quick call this week?"
        ),
        axis=1,
    )
    joined["tier"] = tier
    joined["project_address"] = joined["project_address_selected"]
    return joined[["to_email", "subject", "body", "company", "project_address", "city", "state", "tier"]].drop_duplicates(
        subset=["to_email"]
    )


def run_monday_campaign_fast_path(min_cost: int = MIN_COST_DEFAULT) -> Dict[str, int]:
    df = _load_base_dataset()
    total_records_in = len(df)
    df = _seed_domains_from_file(df)

    selected, exclusion_metrics = _build_company_selection(df, min_cost=min_cost)
    selected.to_csv(OUTPUT_COMPANIES, index=False)
    log.info("Saved %d company rows -> %s", len(selected), OUTPUT_COMPANIES)

    candidates = _build_candidates(selected)
    candidates.to_csv(OUTPUT_CANDIDATES, index=False)
    log.info("Saved %d candidates -> %s", len(candidates), OUTPUT_CANDIDATES)

    verified_valid = pd.DataFrame(columns=list(candidates.columns) + ["email_verification_status"])
    verification_available = bool(os.environ.get("MILLIONVERIFIER_API_KEY", "").strip())
    if verification_available and not candidates.empty:
        try:
            verified = verify_with_millionverifier(candidates)
            verified_valid = candidates.merge(
                verified[verified["email_verification_status"] == "valid"],
                left_on="email_candidate",
                right_on="email",
                how="inner",
            )
        except Exception as exc:
            log.warning("Verification failed, continuing without verified file enrichment: %s", exc)
    verified_valid.to_csv(OUTPUT_VERIFIED, index=False)

    plusvibes_base = verified_valid if not verified_valid.empty else candidates
    plusvibes = _to_plusvibes(plusvibes_base, selected, tier="hot")
    plusvibes.to_csv(OUTPUT_PLUSVIBES, index=False)

    qa = {
        "total_records_in": int(total_records_in),
        "unique_companies_out": int(selected["company_group_key"].nunique() if not selected.empty else 0),
        "email_candidates_generated": int(len(candidates)),
        "verified_valid_count": int(len(verified_valid)),
        "excluded_residential_count": int(exclusion_metrics["excluded_residential_count"]),
        "contacts_per_company_max": MAX_CONTACTS_PER_COMPANY,
        "min_project_cost": int(min_cost),
        "verification_used": verification_available,
    }
    OUTPUT_QA.write_text(json.dumps(qa, indent=2), encoding="utf-8")
    return qa


def main() -> None:
    setup_logging()
    qa = run_monday_campaign_fast_path()
    log.info("Monday fast path complete: %s", qa)


if __name__ == "__main__":
    main()
