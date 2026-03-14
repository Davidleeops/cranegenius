#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple
from urllib.request import Request, urlopen

try:  # pragma: no cover
    from .signal_infrastructure_projects import (
        collect_caltrans_capital_plans,
        collect_federal_earmark_awards,
        collect_infrastructure_announcements,
        collect_txdot_capital_plans,
    )
    from .signal_capital_improvement_plans import (
        collect_fairfax_capital_plan_projects,
        collect_manatee_capital_plan_projects,
        collect_siouxfalls_capital_plan_projects,
        collect_tdot_stip_projects,
    )
    from .signal_zoning_filings import collect_zoning_filings
    from .signal_construction_bids import collect_procurement_rfps
    from .signal_prebid_attendance import collect_prebid_attendance_signals
    from .signal_lift_permits import collect_lift_permits
    from .signal_equipment_rentals import collect_equipment_rental_signals
    from .signal_subcontractor_registrations import collect_subcontractor_registrations
    from .signal_utility_expansion import collect_utility_expansion
    from .signal_utility_infrastructure import collect_utility_infrastructure
    from .signal_utility_irp import collect_utility_irp_signals
    from .signal_oversize_loads import collect_oversize_loads
    from .signal_corporate_capex import collect_corporate_capex_signals
    from .signal_data_center_site_plan_reviews import collect_site_plan_reviews
    from .signal_battery_storage_procurements import collect_bess_procurements
except ImportError:  # pragma: no cover
    from signal_infrastructure_projects import (
        collect_caltrans_capital_plans,
        collect_federal_earmark_awards,
        collect_infrastructure_announcements,
        collect_txdot_capital_plans,
    )
    from signal_capital_improvement_plans import (
        collect_fairfax_capital_plan_projects,
        collect_manatee_capital_plan_projects,
        collect_siouxfalls_capital_plan_projects,
        collect_tdot_stip_projects,
    )
    from signal_zoning_filings import collect_zoning_filings
    from signal_construction_bids import collect_procurement_rfps
    from signal_prebid_attendance import collect_prebid_attendance_signals
    from signal_lift_permits import collect_lift_permits
    from signal_equipment_rentals import collect_equipment_rental_signals
    from signal_subcontractor_registrations import collect_subcontractor_registrations
    from signal_utility_expansion import collect_utility_expansion
    from signal_utility_infrastructure import collect_utility_infrastructure
    from signal_utility_irp import collect_utility_irp_signals
    from signal_oversize_loads import collect_oversize_loads
    from signal_corporate_capex import collect_corporate_capex_signals
    from signal_data_center_site_plan_reviews import collect_site_plan_reviews
    from signal_battery_storage_procurements import collect_bess_procurements


KEYWORD_RE = re.compile(
    r"\b(crane|tower|steel|core|shell|high-rise|high rise|data center|industrial|substation|plant|hospital|airport|stadium|bridge|structural)\b",
    re.IGNORECASE,
)
NOISE_RE = re.compile(
    r"\b(fence|tuck[\s-]?point|roof\s*(tear|recover|repair)|shingle|gutters?|down\s*spout|lintel|awnings?|siding|porch|deck|sidewalk|driveway|garage|carport|sign(age)?|billboard|storefront|patio|chimney|fireplace|kitchen|bath(room)?|remodel|paint|stucco|plaster)\b",
    re.IGNORECASE,
)


def fetch_rows(url: str, query: str = "", timeout: int = 25) -> List[Dict]:
    full_url = url + ("?" + query if query else "")
    req = Request(full_url, headers={"User-Agent": "cranegenius-permit-ingest"})
    with urlopen(req, timeout=timeout) as resp:
        data = json.loads(resp.read().decode("utf-8", errors="ignore"))
    return data if isinstance(data, list) else []


def pick(d: Dict, keys: List[str]) -> str:
    for k in keys:
        v = d.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()
    return ""


def is_crane_candidate(text: str) -> bool:
    haystack = text or ""
    return bool(KEYWORD_RE.search(haystack)) and not NOISE_RE.search(haystack)


def normalize_row(raw: Dict, source: Dict) -> Dict:
    description = pick(raw, ["work_description", "job_description", "permit_type", "description", "project_name"])
    address = pick(raw, ["address", "street_name", "house", "site_address", "job_location"])
    issued = pick(raw, ["issue_date", "filing_date", "permit_issue_date", "created_date"])
    permit_id = pick(raw, ["permit_", "permit_number", "permit_num", "id", "job__", "permitid", "permit_id"])
    haystack = f"{description} {address}"

    return {
        "source": source.get("name", ""),
        "source_url": source.get("url", ""),
        "city": source.get("city", ""),
        "state": source.get("state", ""),
        "permit_id": permit_id,
        "issued_at": issued,
        "address": address,
        "description": description,
        "is_opportunity_candidate": is_crane_candidate(haystack),
        "raw": raw,
    }


def load_previous_rows(path: Path) -> List[Dict]:
    if not path.exists():
        return []
    try:
        old = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if isinstance(old, dict) and isinstance(old.get("rows"), list):
        return old["rows"]
    return []


def main() -> int:
    parser = argparse.ArgumentParser(description="Scrape public permit datasets into a normalized opportunities feed.")
    parser.add_argument("--config", default="config/permit_sources.json", help="Permit source config JSON")
    parser.add_argument("--output", default="data/opportunities/permits_imported.json", help="Output file")
    parser.add_argument(
        "--project-signals-output",
        default="data/project_signals/early_signals.json",
        help="Where to write the aggregated project signal payload.",
    )
    parser.add_argument("--project-signals-limit", type=int, default=125, help="Max rows per signal collector.")
    parser.add_argument("--skip-project-signals", action="store_true", help="Skip refreshing project signals.")
    args = parser.parse_args()

    cfg_path = Path(args.config)
    if not cfg_path.is_absolute():
        cfg_path = Path.cwd() / cfg_path
    config = json.loads(cfg_path.read_text(encoding="utf-8"))
    sources = config.get("sources", [])

    all_rows: List[Dict] = []
    for source in sources:
        rows: List[Dict] = []
        try:
            rows = fetch_rows(source.get("url", ""), source.get("query", ""))
        except Exception:
            rows = []

        # Socrata queries sometimes fail due field-specific order clauses; fallback to simple limit query.
        if not rows:
            try:
                rows = fetch_rows(source.get("url", ""), "$limit=500")
            except Exception:
                rows = []

        for raw in rows:
            all_rows.append(normalize_row(raw, source))

    candidates = [r for r in all_rows if r.get("is_opportunity_candidate")]

    out_path = Path(args.output)
    if not out_path.is_absolute():
        out_path = Path.cwd() / out_path
    out_path.parent.mkdir(parents=True, exist_ok=True)

    previous_rows = load_previous_rows(out_path)
    used_previous = False
    if not candidates and previous_rows:
        candidates = previous_rows
        used_previous = True

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sources_count": len(sources),
        "rows_fetched": len(all_rows),
        "opportunity_candidates": len(candidates),
        "rows": candidates,
        "used_previous_output": used_previous,
    }

    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    msg = f"Wrote {len(candidates)} opportunity candidate permits to {out_path}"
    if used_previous:
        msg += " (preserved previous non-empty output)"
    print(msg)

    if not args.skip_project_signals:
        project_path = Path(args.project_signals_output)
        if not project_path.is_absolute():
            project_path = Path.cwd() / project_path
        rows, stats = collect_project_signals(limit=args.project_signals_limit)
        write_project_signals_output(project_path, rows, stats)
        print(f"Wrote {len(rows)} project signals to {project_path}")
    return 0


def flatten_stats(stats_obj: object, default_source: str) -> List[Dict[str, object]]:
    if isinstance(stats_obj, list):
        return [ensure_source(stat, default_source) for stat in stats_obj]
    if isinstance(stats_obj, dict):
        return [ensure_source(stats_obj, default_source)]
    return [
        {
            "source": default_source,
            "signal_type": default_source,
            "records_returned": 0,
            "empty_feed": True,
            "schema_drift_records": 0,
            "attempts": [],
            "error_categories": [],
            "status": "unknown",
        }
    ]


def ensure_source(stat: Dict[str, object], default_source: str) -> Dict[str, object]:
    if not stat.get("source"):
        stat = {**stat, "source": default_source}
    if not stat.get("signal_type"):
        stat = {**stat, "signal_type": default_source}
    return stat


def collect_project_signals(limit: int = 125) -> Tuple[List[Dict[str, object]], List[Dict[str, object]]]:
    collectors = [
        ("infrastructure_announcements", collect_infrastructure_announcements),
        ("txdot_capital_plans", collect_txdot_capital_plans),
        ("caltrans_capital_plans", collect_caltrans_capital_plans),
        ("fairfax_capital_plan", collect_fairfax_capital_plan_projects),
        ("manatee_capital_plan", collect_manatee_capital_plan_projects),
        ("siouxfalls_capital_plan", collect_siouxfalls_capital_plan_projects),
        ("tdot_stip_plan", collect_tdot_stip_projects),
        ("federal_earmark_awards", collect_federal_earmark_awards),
        ("zoning_filings", collect_zoning_filings),
        ("procurement_rfps", collect_procurement_rfps),
        ("prebid_attendance", collect_prebid_attendance_signals),
        ("lift_permits", collect_lift_permits),
        ("equipment_rentals", collect_equipment_rental_signals),
        ("subcontractor_registrations", collect_subcontractor_registrations),
        ("utility_expansion", collect_utility_expansion),
        ("utility_infrastructure", collect_utility_infrastructure),
        ("utility_irp", collect_utility_irp_signals),
        ("oversize_loads", collect_oversize_loads),
        ("corporate_capex", collect_corporate_capex_signals),
        ("data_center_site_plans", collect_site_plan_reviews),
        ("battery_storage_procurements", collect_bess_procurements),
    ]
    rows: List[Dict[str, object]] = []
    stats: List[Dict[str, object]] = []
    for name, func in collectors:
        try:
            collector_rows, collector_stats = func(limit=limit)
            rows.extend(collector_rows)
            stats.extend(flatten_stats(collector_stats, name))
        except Exception as exc:
            stats.append(
                {
                    "source": name,
                    "signal_type": name,
                    "records_returned": 0,
                    "empty_feed": True,
                    "schema_drift_records": 0,
                    "attempts": [{"status": "error", "reason": str(exc)}],
                    "error_categories": ["collector_exception"],
                }
            )
    return rows, stats


def write_project_signals_output(path: Path, rows: List[Dict[str, object]], stats: List[Dict[str, object]]) -> None:
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "rows": rows,
        "stats": stats,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
