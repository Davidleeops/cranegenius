from __future__ import annotations

import logging
from datetime import datetime
from typing import Tuple

import pandas as pd

log = logging.getLogger("cranegenius.domain_dedupe")


def _to_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", utc=True)


def _explode_domains(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["contractor_domain"] = out.get("contractor_domain", "").fillna("").astype(str)
    out = out.assign(contractor_domain=out["contractor_domain"].str.split("|")).explode("contractor_domain")
    out["contractor_domain"] = out["contractor_domain"].fillna("").astype(str).str.strip().str.lower()
    out = out[out["contractor_domain"] != ""].copy()
    return out


def dedupe_by_domain_signal(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Keep one highest-signal row per domain using cost, keyword score, recency."""
    if df.empty:
        return df.copy(), df.copy()

    expanded = _explode_domains(df)
    if expanded.empty:
        return expanded, expanded

    expanded["project_cost_rank"] = pd.to_numeric(expanded.get("project_cost_optional", 0), errors="coerce").fillna(0)
    expanded["keyword_score_rank"] = pd.to_numeric(expanded.get("keyword_score", expanded.get("lift_probability_score", 0)), errors="coerce").fillna(0)
    expanded["recency_rank"] = _to_datetime(expanded.get("record_date_iso", ""))
    expanded["recency_rank"] = expanded["recency_rank"].fillna(_to_datetime(expanded.get("record_date", "")))
    expanded["recency_rank"] = expanded["recency_rank"].fillna(pd.Timestamp(datetime(1970, 1, 1), tz="UTC"))

    ranked = expanded.sort_values(
        by=["contractor_domain", "project_cost_rank", "keyword_score_rank", "recency_rank"],
        ascending=[True, False, False, False],
    )
    kept = ranked.drop_duplicates(subset=["contractor_domain"], keep="first").copy()
    dropped = ranked[~ranked.index.isin(kept.index)].copy()

    log.info(
        "Domain dedupe: %d rows → %d unique domains (%d dropped)",
        len(expanded),
        len(kept),
        len(dropped),
    )
    return kept, dropped
