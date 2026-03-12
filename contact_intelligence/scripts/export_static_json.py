#!/usr/bin/env python3
"""
Export lightweight static JSON payloads for GitHub Pages frontend consumption.

Current focus:
- Manpower profile cards
- Manpower -> Job match snippets
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

DEFAULT_DB = os.path.expanduser("~/data_runtime/cranegenius_ci.db")


def split_name(full_name: str) -> tuple[str, str]:
    parts = (full_name or "").strip().split()
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])


def initials(first: str, last: str) -> str:
    a = (first[:1] or "?").upper()
    b = (last[:1] or "").upper()
    return (a + (b or "")).strip() or "??"


def avail_bucket(status: str) -> str:
    s = (status or "").lower()
    if "now" in s or "available" in s:
        return "now"
    if "soon" in s:
        return "soon"
    return "employed"


def days_ago(ts: str) -> int:
    if not ts:
        return 7
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        d = (now - dt).days
        return max(1, int(d))
    except Exception:
        return 7


def role_from_title(title: str) -> str:
    t = (title or "").strip()
    return t or "Operator"


def parse_list(text: str) -> List[str]:
    if not text:
        return []
    # Handles comma/pipe/semicolon-separated values.
    chunks = []
    for sep in ["|", ";"]:
        text = text.replace(sep, ",")
    for part in text.split(","):
        p = part.strip()
        if p:
            chunks.append(p)
    return chunks


def fetch_profiles(conn: sqlite3.Connection, limit: int) -> List[Dict]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            manpower_profile_id,
            full_name,
            title,
            certifications,
            location_state,
            availability_status,
            email,
            phone,
            record_quality_score,
            created_at,
            updated_at
        FROM manpower_profiles
        ORDER BY record_quality_score DESC, updated_at DESC
        LIMIT ?
        """,
        (limit,),
    )
    rows = cur.fetchall()

    # Preload matches for performance.
    cur.execute(
        """
        SELECT
            m.manpower_profile_id,
            j.title,
            j.company_name,
            m.match_score,
            m.match_reason
        FROM manpower_job_matches m
        JOIN jobs_feed_items j ON j.job_feed_id = m.job_feed_id
        ORDER BY m.manpower_profile_id, m.match_score DESC
        """
    )
    match_rows = cur.fetchall()
    by_profile: Dict[int, List[Dict]] = {}
    for r in match_rows:
        pid = int(r[0])
        by_profile.setdefault(pid, []).append(
            {
                "title": r[1] or "",
                "company": r[2] or "",
                "score": int(round(float(r[3] or 0.0) * 100)),
                "reason": r[4] or "",
            }
        )

    out: List[Dict] = []
    for r in rows:
        pid = int(r[0])
        full = r[1] or ""
        first, last = split_name(full)
        certs = parse_list(r[3] or "")
        title = role_from_title(r[2] or "")
        state = (r[4] or "").upper().strip()
        location = state if state else "USA"
        quality = float(r[8] or 0.0)
        matches = by_profile.get(pid, [])[:3]
        max_match = max((m["score"] for m in matches), default=0)

        out.append(
            {
                "id": pid,
                "first": first or full or "Unknown",
                "last": last,
                "initials": initials(first or full, last),
                "role": title,
                "location": location,
                "exp": 0,
                "avail": avail_bucket(r[5] or ""),
                "certs": certs,
                "equipment": [],
                "union": "",
                "travel": "Regional",
                "pay": "TBD",
                "bio": f"{title} profile imported from CI data.",
                "verified": bool(quality >= 0.70),
                "daysAgo": days_ago(r[10] or r[9] or ""),
                "matchScore": max_match,
                "matchJobs": matches,
                "source": "ci_manpower_profiles",
                "email": r[6] or "",
                "phone": r[7] or "",
                "quality": round(quality, 3),
            }
        )
    return out


def write_json(path: Path, payload: Dict | List) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def run(db_path: str, output_dir: Path, limit: int) -> None:
    conn = sqlite3.connect(db_path)
    profiles = fetch_profiles(conn, limit=limit)
    conn.close()

    generated_at = datetime.now(timezone.utc).isoformat()

    profiles_path = output_dir / "manpower_profiles.json"
    matches_path = output_dir / "manpower_job_matches.json"
    meta_path = output_dir / "manpower_export_meta.json"

    write_json(profiles_path, {"generated_at": generated_at, "count": len(profiles), "profiles": profiles})

    flattened_matches = []
    for p in profiles:
        for m in p.get("matchJobs", []):
            flattened_matches.append(
                {
                    "profile_id": p["id"],
                    "profile_name": f"{p['first']} {p['last']}".strip(),
                    "job_title": m.get("title", ""),
                    "company": m.get("company", ""),
                    "score": m.get("score", 0),
                    "reason": m.get("reason", ""),
                }
            )
    write_json(matches_path, {"generated_at": generated_at, "count": len(flattened_matches), "matches": flattened_matches})

    write_json(
        meta_path,
        {
            "generated_at": generated_at,
            "paths": {
                "profiles": str(profiles_path),
                "matches": str(matches_path),
            },
            "schema_version": "v1",
            "notes": "Static frontend payload for manpower cards and top match chips.",
        },
    )

    print(f"[static-json] profiles: {len(profiles)} -> {profiles_path}")
    print(f"[static-json] matches: {len(flattened_matches)} -> {matches_path}")
    print(f"[static-json] meta -> {meta_path}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Export static JSON payloads for GitHub Pages frontend.")
    parser.add_argument("--db", default=None, help="Path to cranegenius_ci.db")
    parser.add_argument("--output-dir", default=None, help="Output directory for JSON exports")
    parser.add_argument("--limit", type=int, default=200, help="Max profiles to export")
    args = parser.parse_args()

    db_path = args.db or os.environ.get("CRANEGENIUS_CI_DB", DEFAULT_DB)
    repo_root = Path(__file__).resolve().parents[2]
    output_dir = Path(args.output_dir) if args.output_dir else (repo_root / "data" / "static_exports")

    if not Path(db_path).exists():
        raise SystemExit(f"DB not found: {db_path}")

    run(db_path, output_dir, args.limit)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
