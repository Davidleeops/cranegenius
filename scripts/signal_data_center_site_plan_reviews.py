#!/usr/bin/env python3
"""Collector scaffolding for data center site plan reviews."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple

SIGNAL_FAMILY = "data_center_site_plan_reviews"
SIGNAL_STAGE = "earliest_predictive"
DEFAULT_OUTPUT = Path("data/project_signals/data_center_site_plan_reviews.json")

FALLBACK_ROWS: List[Dict[str, object]] = [
    {
        "agenda": "Loudoun County DRC",
        "project_name": "Ashburn Ridge DC Campus Phase 3",
        "developer": "DigitalForge Partners",
        "city": "Ashburn",
        "state": "VA",
        "megawatts": 96,
        "acreage": 42,
        "meeting_date": "2026-03-18",
        "hearing_type": "site_plan_review",
        "status": "scheduled",
        "link": "https://loudoun.gov/drc/ashburn-ridge-phase-3",
        "notes": "Adds two 4-story data halls with 174' crane clearance requirement.",
    },
    {
        "agenda": "Phoenix Planning Commission",
        "project_name": "Sonoran Switch Data Hub Lot 7",
        "developer": "Canyon Compute",
        "city": "Phoenix",
        "state": "AZ",
        "megawatts": 64,
        "acreage": 28,
        "meeting_date": "2026-03-27",
        "hearing_type": "design_review_board",
        "status": "continued",
        "link": "https://phoenix.gov/planning/agenda/sonoran-switch",
        "notes": "Includes 250-ton crawler picks for chiller yard assemblies.",
    },
]


def normalize_entry(entry: Dict[str, object]) -> Dict[str, object]:
    project = str(entry.get("project_name") or "Data Center Site Plan").strip()
    hearing_type = str(entry.get("hearing_type") or "site_plan_review").strip()
    city = str(entry.get("city") or "").strip()
    state = str(entry.get("state") or "").strip()
    megawatts = float(entry.get("megawatts") or 0)
    acreage = float(entry.get("acreage") or 0)
    meeting_date = str(entry.get("meeting_date") or "").strip()
    confidence = 0.8 if megawatts >= 60 else 0.7
    return {
        "source": "planning_commission_calendar",
        "signal_type": "site_plan_review",
        "signal_family": SIGNAL_FAMILY,
        "signal_stage": SIGNAL_STAGE,
        "project_name": project,
        "project_stage": "entitlements",
        "project_type": "data_center",
        "description": entry.get("notes") or "Data center site plan review",
        "developer": entry.get("developer") or "Unknown",
        "city": city or "Unknown",
        "state": state or "Unknown",
        "meeting_date": meeting_date,
        "agenda_name": entry.get("agenda") or hearing_type,
        "hearing_type": hearing_type,
        "estimated_mw": megawatts,
        "estimated_acreage": acreage,
        "estimated_lift_activity": "high" if megawatts >= 80 else "medium",
        "confidence_score": round(confidence, 2),
        "project_stage_window": "0-12 months",
        "source_url": entry.get("link") or "",
        "external_id": f"{hearing_type}:{project}:{meeting_date}",
        "raw": entry,
    }


def collect_site_plan_reviews(limit: int = 25) -> Tuple[List[Dict[str, object]], Dict[str, object]]:
    sample = FALLBACK_ROWS[:limit] if limit else FALLBACK_ROWS
    rows = [normalize_entry(row) for row in sample]
    stats = {
        "signal_family": SIGNAL_FAMILY,
        "records_returned": len(rows),
        "source": "planning_commission_calendar",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "empty_feed": len(rows) == 0,
    }
    return rows, stats


def write_payload(path: Path, rows: List[Dict[str, object]], stats: Dict[str, object]) -> None:
    payload = {
        "generated_at": stats["generated_at"],
        "signal_family": SIGNAL_FAMILY,
        "signal_stage": SIGNAL_STAGE,
        "rows": rows,
        "stats": stats,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect data center site plan review signals.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Destination JSON path.")
    parser.add_argument("--limit", type=int, default=25, help="Maximum rows to emit.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rows, stats = collect_site_plan_reviews(limit=max(args.limit, 0))
    write_payload(args.output, rows, stats)
    print(f"Wrote {len(rows)} site plan review signals to {args.output}")


if __name__ == "__main__":
    main()
