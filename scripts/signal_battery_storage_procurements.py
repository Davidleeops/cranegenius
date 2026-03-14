#!/usr/bin/env python3
"""Collector scaffolding for utility-scale battery storage procurement notices."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple

SIGNAL_FAMILY = "battery_storage_procurement_notices"
SIGNAL_STAGE = "earliest_predictive"
DEFAULT_OUTPUT = Path("data/project_signals/battery_storage_procurements.json")

FALLBACK_ROWS: List[Dict[str, object]] = [
    {
        "iso": "CAISO",
        "procurement_name": "2026 Resource Adequacy BESS RFO",
        "sponsor": "PG&E",
        "capacity_mw": 400,
        "capacity_mwh": 1600,
        "due_date": "2026-04-05",
        "delivery_year": "2028",
        "location_hint": "Central Valley + LA Basin",
        "status": "open",
        "link": "https://www.pge.com/procurement/bess-rfo-2026",
        "notes": "Requires standalone or paired storage with COD before June 2028.",
    },
    {
        "iso": "NYISO",
        "procurement_name": "Bulk Storage Phase III",
        "sponsor": "NYSERDA",
        "capacity_mw": 275,
        "capacity_mwh": 1100,
        "due_date": "2026-05-12",
        "delivery_year": "2027",
        "location_hint": "Zone J / Zone K priority",
        "status": "draft_rfp",
        "link": "https://www.nyserda.ny.gov/-/media/Files/Programs/Energy-Storage/bulk-storage-phase-3.pdf",
        "notes": "Prefers projects with secured interconnection queue positions.",
    },
]


def normalize_entry(entry: Dict[str, object]) -> Dict[str, object]:
    name = str(entry.get("procurement_name") or "Battery Storage Procurement").strip()
    iso = str(entry.get("iso") or "Unknown").strip()
    sponsor = str(entry.get("sponsor") or "Unknown").strip()
    capacity_mw = float(entry.get("capacity_mw") or 0)
    capacity_mwh = float(entry.get("capacity_mwh") or 0)
    due_date = str(entry.get("due_date") or "").strip()
    status = str(entry.get("status") or "open").strip()
    confidence = 0.85 if capacity_mw >= 250 else 0.72

    return {
        "source": "utility_procurement_portal",
        "signal_type": "battery_storage_procurement",
        "signal_family": SIGNAL_FAMILY,
        "signal_stage": SIGNAL_STAGE,
        "project_name": name,
        "project_stage": "procurement",
        "project_type": "battery_storage",
        "description": entry.get("notes") or "Battery storage solicitation",
        "sponsor": sponsor,
        "grid_operator": iso,
        "due_date": due_date,
        "delivery_year": entry.get("delivery_year") or "",
        "capacity_mw": capacity_mw,
        "capacity_mwh": capacity_mwh,
        "location_hint": entry.get("location_hint") or "",
        "status": status,
        "estimated_lift_activity": "medium" if capacity_mw < 300 else "high",
        "confidence_score": round(confidence, 2),
        "source_url": entry.get("link") or "",
        "external_id": f"{iso}:{name}:{due_date}",
        "raw": entry,
    }


def collect_bess_procurements(limit: int = 25) -> Tuple[List[Dict[str, object]], Dict[str, object]]:
    sample = FALLBACK_ROWS[:limit] if limit else FALLBACK_ROWS
    rows = [normalize_entry(row) for row in sample]
    stats = {
        "signal_family": SIGNAL_FAMILY,
        "source": "utility_procurement_portal",
        "records_returned": len(rows),
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
    parser = argparse.ArgumentParser(description="Collect battery storage procurement notices.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Destination JSON path.")
    parser.add_argument("--limit", type=int, default=25, help="Maximum rows to emit.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rows, stats = collect_bess_procurements(limit=max(args.limit, 0))
    write_payload(args.output, rows, stats)
    print(f"Wrote {len(rows)} battery storage procurement signals to {args.output}")


if __name__ == "__main__":
    main()
