#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List
from urllib.request import Request, urlopen


KEYWORD_RE = re.compile(r"\b(crane|tower|steel|core|shell|high-rise|high rise|data center|industrial|substation|plant|hospital|airport|stadium|bridge|structural)\b", re.IGNORECASE)


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


def normalize_row(raw: Dict, source: Dict) -> Dict:
    description = pick(raw, ["work_description", "job_description", "permit_type", "description", "project_name"])
    address = pick(raw, ["address", "street_name", "house", "site_address", "job_location"])
    issued = pick(raw, ["issue_date", "filing_date", "permit_issue_date", "created_date"])
    permit_id = pick(raw, ["permit_", "permit_number", "permit_num", "id", "job__", "permitid", "permit_id"])

    return {
        "source": source.get("name", ""),
        "source_url": source.get("url", ""),
        "city": source.get("city", ""),
        "state": source.get("state", ""),
        "permit_id": permit_id,
        "issued_at": issued,
        "address": address,
        "description": description,
        "is_opportunity_candidate": bool(KEYWORD_RE.search(f"{description} {address}")),
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
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
