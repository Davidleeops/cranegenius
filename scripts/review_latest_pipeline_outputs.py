#!/usr/bin/env python3
"""
scripts/review_latest_pipeline_outputs.py

Reads the latest pipeline output files and produces:
  1. A concise terminal summary
  2. A markdown report at runs/latest_pipeline_review.md

Does not modify any existing files. No src/ imports.

Usage:
    python3 scripts/review_latest_pipeline_outputs.py
    python3 scripts/review_latest_pipeline_outputs.py --max-sample 10
"""
import argparse
import csv
import json
import os
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Input files
STATS_JSON   = os.path.join(ROOT, "runs", "contact_generation_stats.json")
VERIFY_JSON  = os.path.join(ROOT, "runs", "verification_summary.json")
REPORT_OUT   = os.path.join(ROOT, "runs", "latest_pipeline_review.md")

CSV_FILES = {
    "domains":    os.path.join(ROOT, "data", "monday_company_domains.csv"),
    "people":     os.path.join(ROOT, "data", "monday_people_found.csv"),
    "candidates": os.path.join(ROOT, "data", "monday_all_email_candidates.csv"),
    "valid":      os.path.join(ROOT, "data", "monday_verified_valid.csv"),
    "catchall":   os.path.join(ROOT, "data", "monday_verified_catchall.csv"),
    "invalid":    os.path.join(ROOT, "data", "monday_verified_invalid.csv"),
    "deferred":   os.path.join(ROOT, "data", "monday_verification_deferred.csv"),
}


def load_json(path):
    if not os.path.exists(path):
        return None
    try:
        with open(path) as f:
            return json.load(f)
    except Exception as e:
        return {"_error": str(e)}


def load_csv(path, max_rows=None):
    """Returns (rows_list, total_count, error_string_or_None)."""
    if not os.path.exists(path):
        return [], 0, "FILE NOT FOUND"
    try:
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        total = len(rows)
        sample = rows[:max_rows] if max_rows else rows
        return sample, total, None
    except Exception as e:
        return [], 0, f"READ ERROR: {e}"


def first_meaningful_cols(row, n=3):
    """Return the first n non-empty values from a CSV row as a readable string."""
    vals = [f"{k}={v}" for k, v in row.items() if v and v.strip()]
    return "  |  ".join(vals[:n])


def build_report(args):
    n = args.max_sample
    lines = []  # terminal lines
    md = []     # markdown lines

    # ── Header ──────────────────────────────────────────────────────────────
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    header = f"CraneGenius Pipeline Review — {now}"
    lines.append("\n" + "=" * 60)
    lines.append(header)
    lines.append("=" * 60)
    md.append(f"# CraneGenius Pipeline Review\n")
    md.append(f"**Generated:** {now}\n")

    # ── contact_generation_stats.json ────────────────────────────────────────
    stats = load_json(STATS_JSON)
    md.append("## Run Stats (`contact_generation_stats.json`)\n")
    if stats is None:
        lines.append("\ncontact_generation_stats.json: NOT FOUND")
        md.append("_File not found._\n")
    elif "_error" in stats:
        lines.append(f"\ncontact_generation_stats.json: READ ERROR — {stats['_error']}")
        md.append(f"_Read error: {stats['_error']}_\n")
    else:
        ts = stats.get("timestamp", "unknown")
        companies  = stats.get("companies_processed", "—")
        domains    = stats.get("domains_found", "—")
        generated  = stats.get("emails_generated", "—")
        filtered   = stats.get("emails_filtered", "—")
        ready      = stats.get("emails_ready_for_verification", "—")
        lines.append(f"\nRun timestamp      : {ts}")
        lines.append(f"Companies processed: {companies}")
        lines.append(f"Domains found      : {domains}")
        lines.append(f"Emails generated   : {generated}")
        lines.append(f"Emails filtered    : {filtered}")
        lines.append(f"Ready for verify   : {ready}")
        md.append(f"| Field | Value |\n|---|---|")
        md.append(f"| Run timestamp | {ts} |")
        md.append(f"| Companies processed | {companies} |")
        md.append(f"| Domains found | {domains} |")
        md.append(f"| Emails generated | {generated} |")
        md.append(f"| Emails filtered out | {filtered} |")
        md.append(f"| Ready for verification | {ready} |\n")

    # ── verification_summary.json ────────────────────────────────────────────
    verify = load_json(VERIFY_JSON)
    md.append("## Verification Summary (`verification_summary.json`)\n")
    if verify is None:
        lines.append("\nverification_summary.json: NOT FOUND")
        md.append("_File not found._\n")
    elif "_error" in verify:
        lines.append(f"\nverification_summary.json: READ ERROR — {verify['_error']}")
        md.append(f"_Read error: {verify['_error']}_\n")
    else:
        v_attempted   = verify.get("verification_attempted")
        v_skipped     = verify.get("verification_skipped")
        v_skip_reason = verify.get("skip_reason", "—")
        v_total_sent  = verify.get("total_sent_to_verifier")
        v_valid       = verify.get("valid_count")
        v_catchall    = verify.get("catchall_count")
        v_invalid     = verify.get("invalid_count")
        _fmt = lambda val: str(val) if val is not None else "—"

        lines.append(f"\nVerification attempted : {_fmt(v_attempted)}")
        lines.append(f"Verification skipped   : {_fmt(v_skipped)}  (reason: {v_skip_reason})")
        lines.append(f"Total sent to verifier : {_fmt(v_total_sent)}")
        lines.append(f"  Valid                : {_fmt(v_valid)}")
        lines.append(f"  Catchall             : {_fmt(v_catchall)}")
        lines.append(f"  Invalid              : {_fmt(v_invalid)}")

        md.append(f"| Field | Value |\n|---|---|")
        md.append(f"| Verification attempted | {_fmt(v_attempted)} |")
        md.append(f"| Verification skipped | {_fmt(v_skipped)} (reason: {v_skip_reason}) |")
        md.append(f"| Total sent to verifier | {_fmt(v_total_sent)} |")
        md.append(f"| Valid | {_fmt(v_valid)} |")
        md.append(f"| Catchall | {_fmt(v_catchall)} |")
        md.append(f"| Invalid | {_fmt(v_invalid)} |\n")

    # ── CSV file summaries ───────────────────────────────────────────────────
    md.append("## CSV File Summaries\n")
    lines.append("\n" + "-" * 60)
    lines.append("CSV SUMMARIES")
    lines.append("-" * 60)

    csv_labels = {
        "domains":    ("Company Domains",      "data/monday_company_domains.csv"),
        "people":     ("People Found",         "data/monday_people_found.csv"),
        "candidates": ("All Email Candidates", "data/monday_all_email_candidates.csv"),
        "valid":      ("Verified Valid",       "data/monday_verified_valid.csv"),
        "catchall":   ("Verified Catchall",    "data/monday_verified_catchall.csv"),
        "invalid":    ("Verified Invalid",     "data/monday_verified_invalid.csv"),
        "deferred":   ("Verification Deferred","data/monday_verification_deferred.csv"),
    }

    for key, (label, rel_path) in csv_labels.items():
        path = CSV_FILES[key]
        sample, total, err = load_csv(path, max_rows=n)

        md.append(f"### {label} (`{rel_path}`)\n")
        if err:
            status = f"({err})"
            lines.append(f"\n{label}: {err}")
            md.append(f"_{err}_\n")
            continue

        lines.append(f"\n{label}: {total} rows")
        md.append(f"**Total rows:** {total}\n")

        if total == 0:
            lines.append("  (empty)")
            md.append("_Empty file._\n")
            continue

        lines.append(f"  Sample ({min(n, total)} of {total}):")
        md.append(f"**Sample ({min(n, total)} of {total}):**\n")
        md.append("```")
        for row in sample:
            row_str = first_meaningful_cols(row, n=4)
            lines.append(f"  {row_str}")
            md.append(f"  {row_str}")
        md.append("```\n")

    # ── Footer ───────────────────────────────────────────────────────────────
    lines.append("\n" + "=" * 60)
    lines.append(f"Report saved: runs/latest_pipeline_review.md")
    lines.append("=" * 60 + "\n")
    md.append("---\n")
    md.append(f"_Report generated by scripts/review_latest_pipeline_outputs.py at {now}_\n")

    return "\n".join(lines), "\n".join(md)


def main():
    p = argparse.ArgumentParser(
        description="Summarize latest pipeline outputs to terminal and markdown report."
    )
    p.add_argument(
        "--max-sample", type=int, default=5,
        help="Max rows to show per CSV section (default: 5)"
    )
    args = p.parse_args()

    terminal_out, md_out = build_report(args)

    print(terminal_out)

    os.makedirs(os.path.dirname(REPORT_OUT), exist_ok=True)
    with open(REPORT_OUT, "w", encoding="utf-8") as f:
        f.write(md_out)
    print(f"Markdown report written: {REPORT_OUT}")


if __name__ == "__main__":
    main()
