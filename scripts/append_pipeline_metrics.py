#!/usr/bin/env python3
"""
scripts/append_pipeline_metrics.py

Reads runs/contact_generation_stats.json and runs/verification_summary.json
and appends one row to runs/system_metrics_history.csv.

Writes to the same schema as scripts/append_metrics_history.py so both
scripts produce compatible rows in the same CSV file.

CSV schema (must match system_metrics_history.csv):
  timestamp, dataset, companies, valid_domains, unresolved_domains,
  emails_generated, emails_verified_valid, emails_verified_catchall,
  emails_verified_invalid, notes

Usage:
    python3 scripts/append_pipeline_metrics.py
    python3 scripts/append_pipeline_metrics.py --dataset chicago_nyc --notes "run after domain fix"

Both source JSON files are optional. Missing fields default to 0.
Does not import anything from src/.
"""
import argparse
import csv
import json
import os
from datetime import datetime

ROOT        = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STATS_JSON  = os.path.join(ROOT, "runs", "contact_generation_stats.json")
VERIFY_JSON = os.path.join(ROOT, "runs", "verification_summary.json")
METRICS_CSV = os.path.join(ROOT, "runs", "system_metrics_history.csv")

HEADERS = [
    "timestamp",
    "dataset",
    "companies",
    "valid_domains",
    "unresolved_domains",
    "emails_generated",
    "emails_verified_valid",
    "emails_verified_catchall",
    "emails_verified_invalid",
    "notes",
]


def load_json(path):
    if not os.path.exists(path):
        return {}
    try:
        with open(path) as f:
            return json.load(f)
    except Exception as e:
        print(f"Warning: could not read {path}: {e}")
        return {}


def ensure_csv():
    os.makedirs(os.path.dirname(METRICS_CSV), exist_ok=True)
    if not os.path.exists(METRICS_CSV):
        with open(METRICS_CSV, "w", newline="") as f:
            csv.writer(f).writerow(HEADERS)
        print(f"Created {METRICS_CSV}")


def main():
    p = argparse.ArgumentParser(
        description="Auto-append pipeline run stats to system_metrics_history.csv"
    )
    p.add_argument("--dataset", default="auto", help="Dataset label (default: auto)")
    p.add_argument("--notes", default="", help="Optional run notes")
    args = p.parse_args()

    stats  = load_json(STATS_JSON)
    verify = load_json(VERIFY_JSON)

    # Map JSON fields -> CSV schema columns
    companies     = stats.get("companies_processed", 0)
    valid_domains = stats.get("domains_found", 0)
    unresolved    = companies - valid_domains if companies and valid_domains else 0

    row = {
        "timestamp":               datetime.utcnow().isoformat() + "Z",
        "dataset":                 args.dataset,
        "companies":               companies,
        "valid_domains":           valid_domains,
        "unresolved_domains":      unresolved,
        "emails_generated":        stats.get("emails_generated", 0),
        "emails_verified_valid":   verify.get("valid",    verify.get("emails_verified_valid",    0)),
        "emails_verified_catchall":verify.get("catchall", verify.get("emails_verified_catchall", 0)),
        "emails_verified_invalid": verify.get("invalid",  verify.get("emails_verified_invalid",  0)),
        "notes": args.notes,
    }

    ensure_csv()

    with open(METRICS_CSV, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=HEADERS)
        w.writerow(row)

    print(f"Appended to {METRICS_CSV}")
    print(f"  dataset={row['dataset']}  companies={row['companies']}  "
          f"valid_domains={row['valid_domains']}  unresolved={row['unresolved_domains']}")
    print(f"  emails_generated={row['emails_generated']}  "
          f"valid={row['emails_verified_valid']}  "
          f"catchall={row['emails_verified_catchall']}  "
          f"invalid={row['emails_verified_invalid']}")


if __name__ == "__main__":
    main()
