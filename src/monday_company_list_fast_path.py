from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import pandas as pd

from .utils import normalize_text, setup_logging

log = logging.getLogger("cranegenius.monday_company_list")

DATA_DIR = Path("data")
SCORED_PATH = DATA_DIR / "scored_records.csv"
ENRICHED_PATH = DATA_DIR / "enriched_companies.csv"
NORMALIZED_PATH = DATA_DIR / "normalized_records.csv"

OUT_RANKED = DATA_DIR / "monday_companies_ranked.csv"
OUT_TOP250 = DATA_DIR / "monday_top_250_companies.csv"
OUT_TOP500 = DATA_DIR / "monday_top_500_companies.csv"
OUT_QA = DATA_DIR / "monday_company_qa.json"

EXCLUDE_TERMS = [
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
    "residential",
    "home",
    "remodel existing plumbing",
    "sewer relay",
    "roof only",
]

PREFER_TERMS = [
    "structural steel",
    "curtain wall",
    "glazing",
    "generator",
    "switchgear",
    "rooftop",
    "warehouse",
    "logistics",
    "hospital",
    "data center",
    "manufacturing",
    "mixed-use",
    "commercial",
    "concrete building",
    "caissons",
    "precast",
    "tilt-up",
]

STATUS_BONUS = {
    "issued": 4,
    "approved": 4,
    "awarded": 4,
    "active": 3,
    "finaled": 3,
    "submitted": 1,
    "applied": 1,
    "pending": 0,
}


def _safe_dt(value: str) -> datetime:
    dt = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(dt):
        return datetime(1970, 1, 1, tzinfo=timezone.utc)
    return dt.to_pydatetime()


def _load_base() -> pd.DataFrame:
    if not SCORED_PATH.exists():
        raise FileNotFoundError(f"Missing required file: {SCORED_PATH}")
    df = pd.read_csv(SCORED_PATH)
    if "contractor_name_normalized" not in df.columns:
        df["contractor_name_normalized"] = (
            df.get("contractor_name_raw", "").fillna("").astype(str).str.lower().str.strip()
        )
    if "description_raw" not in df.columns:
        df["description_raw"] = ""
    if "project_city" not in df.columns:
        df["project_city"] = ""
    if "project_state" not in df.columns:
        df["project_state"] = ""
    if "record_status" not in df.columns:
        df["record_status"] = ""
    if "record_date_iso" not in df.columns:
        df["record_date_iso"] = ""
    if "record_date" not in df.columns:
        df["record_date"] = ""
    if "permit_or_record_id" not in df.columns:
        df["permit_or_record_id"] = ""
    if "lift_probability_score" not in df.columns:
        df["lift_probability_score"] = 0
    df["contractor_domain"] = ""
    return df


def _merge_optional_domains(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    # Prefer enriched_companies because it contains scored+resolved rows.
    if ENRICHED_PATH.exists():
        enriched = pd.read_csv(ENRICHED_PATH)
        if "contractor_domain" in enriched.columns:
            cols = [c for c in ["dedupe_key", "contractor_name_normalized", "contractor_domain"] if c in enriched.columns]
            enriched = enriched[cols].drop_duplicates()
            if "dedupe_key" in out.columns and "dedupe_key" in enriched.columns:
                out = out.merge(
                    enriched[["dedupe_key", "contractor_domain"]],
                    on="dedupe_key",
                    how="left",
                    suffixes=("", "_enriched"),
                )
                out["contractor_domain"] = out["contractor_domain_enriched"].fillna(out["contractor_domain"])
                out = out.drop(columns=["contractor_domain_enriched"])
            if out["contractor_domain"].eq("").all() and "contractor_name_normalized" in enriched.columns:
                name_domain = (
                    enriched[["contractor_name_normalized", "contractor_domain"]]
                    .dropna()
                    .drop_duplicates(subset=["contractor_name_normalized"])
                )
                out = out.merge(
                    name_domain,
                    on="contractor_name_normalized",
                    how="left",
                    suffixes=("", "_name"),
                )
                out["contractor_domain"] = out["contractor_domain_name"].fillna(out["contractor_domain"])
                out = out.drop(columns=["contractor_domain_name"])
    return out


def _count_terms(text: str, terms: List[str]) -> int:
    low = normalize_text(text).lower()
    return sum(1 for t in terms if t in low)


def _rank(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["description_raw"] = out["description_raw"].fillna("").astype(str)
    out["record_status"] = out["record_status"].fillna("").astype(str).str.lower()
    out["lift_probability_score"] = pd.to_numeric(out["lift_probability_score"], errors="coerce").fillna(0)
    out["excluded_residential"] = out["description_raw"].map(lambda x: _count_terms(x, EXCLUDE_TERMS) > 0)
    out["positive_hits"] = out["description_raw"].map(lambda x: _count_terms(x, PREFER_TERMS))
    out["status_bonus"] = out["record_status"].map(lambda s: STATUS_BONUS.get(s, 0))
    out["recency_ts"] = out["record_date_iso"].fillna("").map(_safe_dt)
    missing = out["recency_ts"] == datetime(1970, 1, 1, tzinfo=timezone.utc)
    if missing.any():
        out.loc[missing, "recency_ts"] = out.loc[missing, "record_date"].fillna("").map(_safe_dt)
    out["recency_days"] = (datetime.now(timezone.utc) - out["recency_ts"]).dt.days.clip(lower=0)
    out["recency_bonus"] = (30 - out["recency_days"]).clip(lower=0, upper=30) / 10.0

    out["company_key"] = out["contractor_domain"].fillna("").astype(str).str.strip().str.lower()
    missing_key = out["company_key"] == ""
    out.loc[missing_key, "company_key"] = (
        out.loc[missing_key, "contractor_name_normalized"].fillna("").astype(str).str.lower().str.strip()
    )
    out = out[out["company_key"] != ""].copy()

    out["rank_score"] = (
        out["lift_probability_score"] * 10
        + out["positive_hits"] * 6
        + out["status_bonus"] * 2
        + out["recency_bonus"]
    )
    # Prefer stronger commercial/industrial rows before company dedupe.
    out = out[(~out["excluded_residential"]) & (out["positive_hits"] > 0)].copy()
    ranked = out.sort_values(
        by=["company_key", "rank_score", "lift_probability_score", "positive_hits", "status_bonus", "recency_ts"],
        ascending=[True, False, False, False, False, False],
    )
    selected = ranked.drop_duplicates(subset=["company_key"], keep="first").copy()
    return selected.sort_values(by=["rank_score", "lift_probability_score"], ascending=[False, False]).copy()


def run() -> Dict[str, int]:
    base = _load_base()
    total_input_rows = len(base)
    base = _merge_optional_domains(base)

    excluded_residential_count = int(
        base["description_raw"].fillna("").astype(str).map(lambda x: _count_terms(x, EXCLUDE_TERMS) > 0).sum()
    )
    ranked = _rank(base)

    ranked_out = pd.DataFrame(
        {
            "contractor_name_normalized": ranked.get("contractor_name_normalized", ""),
            "contractor_domain": ranked.get("contractor_domain", ""),
            "best_project_description": ranked.get("description_raw", ""),
            "project_city": ranked.get("project_city", ""),
            "project_state": ranked.get("project_state", ""),
            "lift_probability_score": ranked.get("lift_probability_score", 0),
            "permit_or_record_id": ranked.get("permit_or_record_id", ""),
            "record_status": ranked.get("record_status", ""),
            "rank_score": ranked.get("rank_score", 0),
            "positive_hits": ranked.get("positive_hits", 0),
        }
    )
    ranked_out.to_csv(OUT_RANKED, index=False)

    top250 = ranked_out.head(250).copy()
    top500 = ranked_out.head(500).copy()
    top250.to_csv(OUT_TOP250, index=False)
    top500.to_csv(OUT_TOP500, index=False)

    qa = {
        "total_input_rows": int(total_input_rows),
        "total_unique_companies": int(base["company_key"].nunique() if "company_key" in base.columns else base["contractor_name_normalized"].nunique()),
        "excluded_residential_count": int(excluded_residential_count),
        "remaining_ranked_companies": int(len(ranked_out)),
        "top_250_count": int(len(top250)),
        "top_500_count": int(len(top500)),
    }
    OUT_QA.write_text(json.dumps(qa, indent=2), encoding="utf-8")
    return qa


def main() -> None:
    setup_logging()
    qa = run()
    log.info("Monday company fast path complete: %s", qa)


if __name__ == "__main__":
    main()
