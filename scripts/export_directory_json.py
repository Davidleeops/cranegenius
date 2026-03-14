#!/usr/bin/env python3
"""
Export CraneGenius supplier directory data for the public directory page.

Primary data source is the contact_intelligence SQLite database. When a company
match is found, we enrich the curated supplier seed with match counts and the
latest priority scores so front-end consumers can highlight active partners.
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional


NAME_NORMALIZE_RE = re.compile(r"[^a-z0-9]+")


def normalize_name(value: Optional[str]) -> str:
    if not value:
        return ""
    return NAME_NORMALIZE_RE.sub("", value.lower())


def load_seed(seed_path: Path) -> List[Dict[str, object]]:
    if not seed_path.exists():
        return []
    try:
        payload = json.loads(seed_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []
    suppliers = payload.get("suppliers") if isinstance(payload, dict) else None
    return suppliers if isinstance(suppliers, list) else []


def fetch_company_index(conn: sqlite3.Connection) -> Dict[str, Dict[str, object]]:
    index: Dict[str, Dict[str, object]] = {}
    cur = conn.execute(
        """
        SELECT company_id, company_name, location_city, location_state, target_score, priority_reason, updated_at
        FROM companies
        WHERE company_name IS NOT NULL
        """
    )
    for row in cur.fetchall():
        key = normalize_name(row[1])
        if not key:
            continue
        record = {
            "company_id": int(row[0]),
            "name": row[1] or "",
            "city": row[2] or "",
            "state": row[3] or "",
            "target_score": row[4] if row[4] is not None else None,
            "priority_reason": row[5] or "",
            "updated_at": row[6] or "",
        }
        existing = index.get(key)
        if not existing or (record["target_score"] or 0) > (existing.get("target_score") or 0):
            index[key] = record
    return index


def fetch_opportunity_counts(conn: sqlite3.Connection) -> Dict[int, int]:
    cur = conn.execute("SELECT company_id, COUNT(*) FROM opportunity_company_matches GROUP BY company_id")
    return {int(row[0]): int(row[1]) for row in cur.fetchall() if row[0] is not None}


def enrich_supplier(
    supplier: Dict[str, object], company_index: Dict[str, Dict[str, object]], match_counts: Dict[int, int]
) -> Dict[str, object]:
    enriched = dict(supplier)
    normalized = normalize_name(str(enriched.get("company_name", "")))
    company = company_index.get(normalized)
    metrics = {
        "matched_company_id": None,
        "target_score": None,
        "priority_reason": "",
        "last_updated_at": "",
        "opportunity_matches": 0,
    }
    if company:
        metrics["matched_company_id"] = company["company_id"]
        metrics["target_score"] = company["target_score"]
        metrics["priority_reason"] = company["priority_reason"]
        metrics["last_updated_at"] = company["updated_at"]
        metrics["opportunity_matches"] = match_counts.get(company["company_id"], 0)
        if not enriched.get("city"):
            enriched["city"] = company.get("city", "")
        if not enriched.get("state_or_province"):
            enriched["state_or_province"] = company.get("state", "")
    enriched["metrics"] = metrics
    return enriched


def build_summary(entries: List[Dict[str, object]]) -> Dict[str, object]:
    active = sum(1 for entry in entries if entry.get("active_status", True))
    by_company_type = Counter(entry.get("company_type", "unspecified") or "unspecified" for entry in entries)
    return {
        "active_suppliers": active,
        "by_company_type": dict(sorted(by_company_type.items(), key=lambda item: item[0])),
    }


def build_directory_payload(db_path: Path, seed_path: Path) -> Dict[str, object]:
    suppliers = load_seed(seed_path)
    company_index: Dict[str, Dict[str, object]] = {}
    match_counts: Dict[int, int] = {}
    db_exists = db_path.exists()

    if db_exists:
        conn = sqlite3.connect(str(db_path))
        try:
            conn.row_factory = sqlite3.Row
            company_index = fetch_company_index(conn)
            match_counts = fetch_opportunity_counts(conn)
        finally:
            conn.close()

    enriched = [enrich_supplier(supplier, company_index, match_counts) for supplier in suppliers]
    summary = build_summary(enriched)
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_db": str(db_path),
        "seed_path": str(seed_path),
        "db_connected": db_exists,
        "count": len(enriched),
        "suppliers": enriched,
        "summary": summary,
    }
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Export CraneGenius supplier directory JSON.")
    parser.add_argument("--db", default=str(Path.home() / "data_runtime" / "cranegenius_ci.db"), help="SQLite database path")
    parser.add_argument(
        "--seed",
        default=str(Path("data") / "suppliers" / "suppliers_seed.json"),
        help="Curated seed file for supplier metadata.",
    )
    parser.add_argument(
        "--output",
        default=str(Path("data") / "static_exports" / "directory_suppliers.json"),
        help="Output JSON path",
    )
    args = parser.parse_args()

    db_path = Path(args.db).expanduser()
    seed_path = Path(args.seed).expanduser()
    output_path = Path(args.output).expanduser()
    payload = build_directory_payload(db_path, seed_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Wrote {payload['count']} supplier entries to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
