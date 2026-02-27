from __future__ import annotations

import logging
from typing import Dict, Tuple

import pandas as pd

from .utils import safe_json_dumps

log = logging.getLogger("cranegenius.export")

VALID_STATUSES = {"valid"}  # Only hard valid for export — catchall goes to separate review list


def export_sender_lists(
    scored_enriched_df: pd.DataFrame,
    verified_df: pd.DataFrame,
    threshold_hot: int = 7,
    threshold_warm: int = 5,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, Dict]:
    """
    Returns: hot_df, warm_df, catchall_review_df, qa_dict
    """
    df = scored_enriched_df.copy()

    # Merge verification results
    if "email_candidate" in df.columns and not verified_df.empty:
        df = df.merge(
            verified_df,
            left_on="email_candidate",
            right_on="email",
            how="left"
        )
    else:
        df["email_verification_status"] = "unverified"
        df["email_is_catchall"] = False

    df["email_verification_status"] = df["email_verification_status"].fillna("unknown")
    df["email_is_catchall"] = df.get("email_is_catchall", False).fillna(False)

    # Add personalization tokens for every row
    df["personalization_tokens"] = df.apply(lambda r: safe_json_dumps({
        "jurisdiction": r.get("jurisdiction", ""),
        "city": r.get("project_city", ""),
        "state": r.get("project_state", ""),
        "trigger": r.get("score_hits", ""),
        "status": r.get("record_status", ""),
        "address": r.get("project_address", ""),
        "permit_id": r.get("permit_or_record_id", ""),
    }), axis=1)

    # Segment: hot = score 7+, valid email
    hot = df[
        (df["lift_probability_score"] >= threshold_hot) &
        (df["email_verification_status"].isin(VALID_STATUSES))
    ].copy()

    # Segment: warm = score 5+, valid email (includes hot)
    warm = df[
        (df["lift_probability_score"] >= threshold_warm) &
        (df["lift_probability_score"] < threshold_hot) &
        (df["email_verification_status"].isin(VALID_STATUSES))
    ].copy()

    # Catchall review — scored 5+ but email is catchall (your call on sending)
    catchall_review = df[
        (df["lift_probability_score"] >= threshold_warm) &
        (df["email_is_catchall"] == True)
    ].copy()

    # QA summary
    total = len(df)
    valid_count = (df["email_verification_status"].isin(VALID_STATUSES)).sum()
    valid_rate = valid_count / total if total else 0

    qa = {
        "total_scored_enriched": int(total),
        "total_with_domain": int((df.get("contractor_domain", "") != "").sum()),
        "total_verified_valid": int(valid_count),
        "valid_email_rate": round(float(valid_rate), 3),
        "hot_count": int(len(hot)),
        "warm_count": int(len(warm)),
        "catchall_review_count": int(len(catchall_review)),
        "gate_email_rate_ok": bool(valid_rate >= 0.30),
        "gate_note": "" if valid_rate >= 0.30 else "WARNING: valid email rate below 30% — review before sending",
    }

    log.info(
        "Export: hot=%d, warm=%d, catchall_review=%d | valid rate=%.0f%%",
        len(hot), len(warm), len(catchall_review), valid_rate * 100,
    )

    if not qa["gate_email_rate_ok"]:
        log.warning("GATE TRIGGERED: %s", qa["gate_note"])

    return hot, warm, catchall_review, qa
