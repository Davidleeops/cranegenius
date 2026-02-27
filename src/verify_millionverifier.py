from __future__ import annotations

import logging
import os
from typing import Any, Dict, List

import pandas as pd
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from .utils import normalize_text

log = logging.getLogger("cranegenius.verify")

MV_API_URL = "https://api.millionverifier.com/api/v3/"

# MillionVerifier result values that mean "safe to send"
VALID_STATUSES = {"ok", "valid", "verified", "deliverable", "good"}
CATCHALL_STATUSES = {"catchall", "catch_all", "accept_all"}


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=8))
def _verify_one(api_key: str, email: str) -> Dict[str, Any]:
    resp = requests.get(
        MV_API_URL,
        params={"api": api_key, "email": email},
        timeout=25,
    )
    resp.raise_for_status()
    return resp.json()


def verify_with_millionverifier(candidates_df: pd.DataFrame) -> pd.DataFrame:
    api_key = os.environ.get("MILLIONVERIFIER_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError(
            "MILLIONVERIFIER_API_KEY environment variable is not set. "
            "Add it as a GitHub Secret."
        )

    rows: List[Dict[str, Any]] = []
    total = len(candidates_df)
    log.info("Verifying %d email candidates with MillionVerifier...", total)

    for i, (_, r) in enumerate(candidates_df.iterrows()):
        email = normalize_text(r.get("email_candidate")).lower()
        if not email:
            continue

        if i % 20 == 0:
            log.info("  Verified %d/%d...", i, total)

        status = "unknown"
        is_catchall = False
        quality_score = None

        try:
            data = _verify_one(api_key, email)
            raw_result = normalize_text(data.get("result") or data.get("result_code") or "").lower()
            quality_score = data.get("quality")

            if raw_result in VALID_STATUSES:
                status = "valid"
            elif raw_result in CATCHALL_STATUSES:
                status = "catchall"
                is_catchall = True
            elif raw_result in {"invalid", "bad", "undeliverable"}:
                status = "invalid"
            else:
                status = raw_result or "unknown"

        except Exception as exc:
            log.debug("Verification failed for %s: %s", email, exc)
            status = "error"

        rows.append({
            "email": email,
            "email_verification_status": status,
            "email_is_catchall": is_catchall,
            "email_quality_score": quality_score,
            "email_verification_provider": "millionverifier",
        })

    result_df = pd.DataFrame(rows).drop_duplicates(subset=["email"])

    # Log verification summary
    valid_count = (result_df["email_verification_status"] == "valid").sum() if "email_verification_status" in result_df.columns else 0
    catchall_count = (result_df["email_verification_status"] == "catchall").sum()
    invalid_count = (result_df["email_verification_status"] == "invalid").sum()
    total_verified = len(result_df)

    valid_rate = valid_count / total_verified if total_verified else 0
    log.info(
        "Verification complete: valid=%d (%.0f%%), catchall=%d, invalid=%d",
        valid_count, valid_rate * 100, catchall_count, invalid_count,
    )

    return result_df
