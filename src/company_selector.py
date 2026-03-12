from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Dict, List, Tuple

import pandas as pd

from .utils import normalize_text

log = logging.getLogger("cranegenius.company_selector")

DEFAULT_EXCLUSION_TERMS = [
    "single-family",
    "single family",
    "townhome",
    "townhouse",
    "duplex",
    "garage",
    "porch",
    "fence",
    "basement",
    "residential rehab",
    "minor addition",
]

COMMERCIAL_SIGNAL_TERMS = [
    "data center",
    "hospital",
    "industrial",
    "manufacturing",
    "tower",
    "utility",
    "substation",
    "warehouse",
    "distribution",
    "plant",
    "infrastructure",
    "commercial",
    "transformer",
]


def _to_dt(value: str) -> datetime:
    if not value:
        return datetime(1970, 1, 1, tzinfo=timezone.utc)
    dt = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(dt):
        return datetime(1970, 1, 1, tzinfo=timezone.utc)
    return dt.to_pydatetime()


def _primary_domain(value: str) -> str:
    v = normalize_text(value).lower()
    if not v:
        return ""
    return v.split("|")[0].strip()


def _keyword_strength(row: pd.Series) -> int:
    text = " ".join(
        [
            normalize_text(row.get("description_raw")).lower(),
            normalize_text(row.get("signal_keywords")).lower(),
            normalize_text(row.get("score_hits")).lower(),
        ]
    )
    return sum(1 for term in COMMERCIAL_SIGNAL_TERMS if term in text)


def _excluded_residential(row: pd.Series, exclusion_terms: List[str]) -> bool:
    text = normalize_text(row.get("description_raw")).lower()
    return any(term in text for term in exclusion_terms)


def _apply_send_priority(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["project_cost_optional"] = pd.to_numeric(out.get("project_cost_optional", 0), errors="coerce").fillna(0)
    out["lift_probability_score"] = pd.to_numeric(out.get("lift_probability_score", 0), errors="coerce").fillna(0)
    out["keyword_strength"] = out.apply(_keyword_strength, axis=1)
    out["recency_ts"] = out.get("record_date_iso", "").fillna(out.get("record_date", "")).map(_to_dt)
    out["recency_days"] = (datetime.now(timezone.utc) - out["recency_ts"]).dt.days.clip(lower=0)
    out["recency_bonus"] = (30 - out["recency_days"]).clip(lower=0, upper=30) / 10.0
    out["outbound_priority_score"] = (
        out["lift_probability_score"] * 3
        + (out["keyword_strength"] * 2)
        + (out["project_cost_optional"] / 1_000_000.0).clip(upper=40)
        + out["recency_bonus"]
    )
    return out


def select_companies_for_send(
    enriched_df: pd.DataFrame,
    threshold_hot: int = 7,
    min_cost: int = 2_000_000,
    exclusion_terms: List[str] | None = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, float]]:
    """Company-level selector between scoring and contact generation."""
    if enriched_df.empty:
        return enriched_df.copy(), enriched_df.copy(), {
            "total_unique_companies": 0,
            "total_unique_domains": 0,
            "avg_rows_per_company_before_dedupe": 0.0,
            "send_ready_companies": 0,
            "excluded_residential_count": 0,
        }

    terms = [normalize_text(t).lower() for t in (exclusion_terms or DEFAULT_EXCLUSION_TERMS) if normalize_text(t)]
    working = _apply_send_priority(enriched_df)
    working["primary_domain"] = working.get("contractor_domain", "").map(_primary_domain)
    working["company_group_key"] = working["primary_domain"]
    missing_domain = working["company_group_key"] == ""
    working.loc[missing_domain, "company_group_key"] = working.loc[missing_domain, "contractor_name_normalized"].fillna("").astype(str).str.lower().str.strip()
    working["company_group_key"] = working["company_group_key"].replace("", "unknown")

    before_rows = len(working)
    unique_companies_before = working["company_group_key"].nunique()
    unique_domains_before = working.loc[working["primary_domain"] != "", "primary_domain"].nunique()

    working["excluded_residential"] = working.apply(lambda r: _excluded_residential(r, terms), axis=1)
    working["cost_numeric"] = pd.to_numeric(working.get("project_cost_optional", 0), errors="coerce").fillna(0)
    working["passes_cost_rule"] = (working["cost_numeric"] >= float(min_cost)) | (
        (working["cost_numeric"] <= 0)
        & (working["lift_probability_score"] >= float(threshold_hot))
        & (working["keyword_strength"] >= 2)
    )

    excluded = working[(working["excluded_residential"]) | (~working["passes_cost_rule"])].copy()
    kept = working[(~working["excluded_residential"]) & (working["passes_cost_rule"])].copy()

    ranked = kept.sort_values(
        by=["company_group_key", "lift_probability_score", "cost_numeric", "recency_ts", "keyword_strength", "outbound_priority_score"],
        ascending=[True, False, False, False, False, False],
    )
    selected = ranked.drop_duplicates(subset=["company_group_key"], keep="first").copy()

    metrics = {
        "total_unique_companies": int(unique_companies_before),
        "total_unique_domains": int(unique_domains_before),
        "avg_rows_per_company_before_dedupe": round(before_rows / unique_companies_before, 3) if unique_companies_before else 0.0,
        "send_ready_companies": int(selected["company_group_key"].nunique()),
        "excluded_residential_count": int(excluded["excluded_residential"].sum()),
    }
    log.info(
        "Company selector: rows=%d -> selected=%d, unique_companies=%d, excluded_residential=%d",
        before_rows,
        len(selected),
        metrics["total_unique_companies"],
        metrics["excluded_residential_count"],
    )
    return selected, excluded, metrics
