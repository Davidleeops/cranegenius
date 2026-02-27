"""
Pipeline monitoring and gate enforcement.

Gates are hard stops — if a gate fails, the pipeline halts before
exporting and writing to sender lists. This prevents bad data from
reaching your email sender.
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict

import pandas as pd

log = logging.getLogger("cranegenius.monitor")

STATE_FILE = "data/pipeline_state.json"


def load_state() -> Dict[str, Any]:
    """Load persistent state between runs (tracks consecutive failures per source)."""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    return {"consecutive_zero_runs": {}, "run_history": []}


def save_state(state: Dict[str, Any]) -> None:
    os.makedirs("data", exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def check_gates(qa: Dict[str, Any], scoring_yaml_cfg: Dict[str, Any]) -> Dict[str, Any]:
    """
    Run all monitoring gates against the QA report.
    Returns a gate_report dict. If gate_report['halt'] is True, stop before export.
    """
    gates_cfg = scoring_yaml_cfg.get("scoring", {}).get("gates", {})
    min_valid_rate = float(gates_cfg.get("min_valid_email_rate", 0.30))
    min_domain_rate = float(gates_cfg.get("min_domain_resolution_rate", 0.25))

    failures = []
    warnings = []

    # Gate 1: Valid email rate
    valid_rate = float(qa.get("valid_email_rate", 0))
    total = int(qa.get("total_scored_enriched", 0))

    if total > 10:  # Only gate if we have meaningful sample size
        if valid_rate < min_valid_rate:
            failures.append(
                f"Valid email rate {valid_rate:.1%} is below minimum {min_valid_rate:.1%}. "
                f"Check MillionVerifier API key and domain resolution quality."
            )
    elif total > 0:
        warnings.append(f"Small sample size ({total} records) — gates relaxed.")

    # Gate 2: Domain resolution rate
    total_records = int(qa.get("total_scored_enriched", 0))
    total_with_domain = int(qa.get("total_with_domain", 0))
    if total_records > 10:
        domain_rate = total_with_domain / total_records if total_records else 0
        if domain_rate < min_domain_rate:
            warnings.append(
                f"Domain resolution rate {domain_rate:.1%} is below {min_domain_rate:.1%}. "
                f"Expand company_domain_seed.csv."
            )

    # Gate 3: Zero output
    if total == 0:
        failures.append("Pipeline produced zero scored records. Check source configs and keyword matches.")

    halt = len(failures) > 0
    gate_report = {
        "halt": halt,
        "failures": failures,
        "warnings": warnings,
        "valid_email_rate": valid_rate,
    }

    if halt:
        for f in failures:
            log.error("GATE FAILURE: %s", f)
        log.error("Pipeline halted before export. Fix issues above before sending.")
    else:
        for w in warnings:
            log.warning("GATE WARNING: %s", w)
        log.info("All gates passed.")

    return gate_report


def update_source_state(raw_df: pd.DataFrame, sources: list, state: Dict[str, Any]) -> Dict[str, Any]:
    """Track which sources are returning zero records and quarantine after threshold."""
    max_zero_runs = 2
    quarantine_list = state.get("quarantined_sources", [])

    for source in sources:
        source_id = source.get("id", "")
        source_rows = len(raw_df[raw_df.get("source_id", pd.Series()) == source_id]) if "source_id" in raw_df.columns else 0

        zeros = state.get("consecutive_zero_runs", {})
        if source_rows == 0:
            zeros[source_id] = zeros.get(source_id, 0) + 1
            log.warning("Source '%s' returned 0 records (run %d consecutive)", source_id, zeros[source_id])
            if zeros[source_id] >= max_zero_runs and source_id not in quarantine_list:
                quarantine_list.append(source_id)
                log.error(
                    "QUARANTINE: Source '%s' has returned 0 records for %d consecutive runs. "
                    "Check if the portal URL has changed.",
                    source_id, zeros[source_id]
                )
        else:
            zeros[source_id] = 0  # Reset on success

        state["consecutive_zero_runs"] = zeros
        state["quarantined_sources"] = quarantine_list

    return state
