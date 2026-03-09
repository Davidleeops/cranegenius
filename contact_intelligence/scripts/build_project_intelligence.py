#!/usr/bin/env python3
"""
Build Project Intelligence layer from currently available runtime data.

Sources in this pass:
- data/opportunities/permits_imported.json
- data/jobs_imported.json
- existing DB signals/projects tables

No frontend dependencies, additive schema only.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple

DEFAULT_DB = os.path.expanduser("~/data_runtime/cranegenius_ci.db")

VERTICAL_KEYWORDS = {
    "data_centers": ["data center", "datacenter", "colo", "hyperscale"],
    "power_energy": ["power", "substation", "utility", "energy", "solar", "wind", "battery", "lng", "gas", "nuclear"],
    "industrial_manufacturing": ["industrial", "manufacturing", "plant", "refinery", "petrochemical", "mill", "factory"],
    "warehousing_logistics": ["warehouse", "distribution", "logistics", "fulfillment", "cold storage"],
    "ports_terminals": ["port", "terminal", "marine", "shipyard", "harbor"],
    "airports": ["airport", "aviation", "runway", "terminal expansion"],
    "healthcare_hospitals": ["hospital", "medical center", "healthcare", "clinic"],
    "semi_battery_ev": ["semiconductor", "chip", "fab", "battery", "ev", "gigafactory"],
    "rail_transit_bridge_infra": ["rail", "transit", "bridge", "highway", "infrastructure", "interchange"],
    "institutional_campus_stadium": ["campus", "stadium", "university", "institutional", "arena", "school"],
}

SPEND_BASE = {
    "semi_battery_ev": 96,
    "data_centers": 94,
    "power_energy": 91,
    "rail_transit_bridge_infra": 89,
    "ports_terminals": 87,
    "industrial_manufacturing": 85,
    "airports": 84,
    "healthcare_hospitals": 82,
    "warehousing_logistics": 80,
    "institutional_campus_stadium": 78,
    "other": 62,
}

CRANE_RELEVANCE_BASE = {
    "data_centers": 88,
    "power_energy": 86,
    "industrial_manufacturing": 87,
    "warehousing_logistics": 79,
    "ports_terminals": 84,
    "airports": 82,
    "healthcare_hospitals": 72,
    "semi_battery_ev": 90,
    "rail_transit_bridge_infra": 89,
    "institutional_campus_stadium": 75,
    "other": 60,
}

MINI_KEYWORDS = [
    "mini crane",
    "spider",
    "tight access",
    "tight access lift",
    "constrained",
    "curtain wall",
    "glazing",
    "glass",
    "glass installation",
    "storefront",
    "facade",
    "window replacement",
    "rooftop",
    "rooftop hvac",
    "rooftop unit",
    "rtu replacement",
    "hvac replacement",
    "mechanical retrofit",
    "hospital retrofit",
    "interior",
    "interior equipment",
    "equipment install",
    "air handler",
    "chiller replacement",
    "mechanical equipment",
    "mech",
    "mep",
    "retrofit",
    "urban infill",
    "inside",
]

STRONG_MINI_KEYWORDS = [
    "mini crane",
    "spider",
    "tight access",
    "tight access lift",
    "constrained",
    "curtain wall",
    "glazing",
    "glass installation",
    "storefront",
    "facade",
    "window replacement",
    "rooftop hvac",
    "rooftop unit",
    "rtu replacement",
    "hvac replacement",
    "mechanical retrofit",
    "hospital retrofit",
    "interior equipment",
    "equipment install",
    "chiller replacement",
    "air handler",
]

SOURCE_CONFIDENCE_BONUS = {
    "permits_imported": 0.08,
    "db_signals": 0.05,
    "jobs_imported": -0.04,
}


def norm_text(v: object) -> str:
    return str(v or "").strip()


def norm_key(v: str) -> str:
    v = norm_text(v).lower()
    v = re.sub(r"[^a-z0-9\s]", " ", v)
    return re.sub(r"\s+", " ", v).strip()


def hash_key(*parts: str) -> str:
    payload = "|".join(norm_key(p) for p in parts)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:20]


def load_json(path: Path) -> Dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def apply_schema(conn: sqlite3.Connection, schema_file: Path) -> None:
    sql = schema_file.read_text(encoding="utf-8")
    conn.executescript(sql)
    conn.commit()


def start_run(cur: sqlite3.Cursor, source_name: str, source_type: str, notes: str = "") -> int:
    cur.execute(
        """
        INSERT INTO signal_source_runs (source_name, source_type, run_started_at, status, notes)
        VALUES (?,?,CURRENT_TIMESTAMP,'running',?)
        """,
        (source_name, source_type, notes),
    )
    return int(cur.lastrowid)


def finish_run(cur: sqlite3.Cursor, run_id: int, seen: int, ingested: int, status: str = "success") -> None:
    cur.execute(
        """
        UPDATE signal_source_runs
        SET run_finished_at=CURRENT_TIMESTAMP,
            records_seen=?,
            records_ingested=?,
            status=?
        WHERE signal_source_run_id=?
        """,
        (seen, ingested, status, run_id),
    )


def insert_signal_event(cur: sqlite3.Cursor, event: Dict) -> None:
    cur.execute(
        """
        INSERT INTO signal_events (
            signal_key, source_name, source_record_id, source_url, signal_type,
            project_name_raw, company_name_raw, city, state, observed_at,
            effective_date, confidence_score, raw_payload_ref, raw_payload
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(signal_key) DO UPDATE SET
            source_url=excluded.source_url,
            signal_type=excluded.signal_type,
            project_name_raw=excluded.project_name_raw,
            company_name_raw=excluded.company_name_raw,
            city=excluded.city,
            state=excluded.state,
            observed_at=excluded.observed_at,
            effective_date=excluded.effective_date,
            confidence_score=excluded.confidence_score,
            raw_payload_ref=excluded.raw_payload_ref,
            raw_payload=excluded.raw_payload,
            updated_at=CURRENT_TIMESTAMP
        """,
        (
            event["signal_key"],
            event["source_name"],
            event.get("source_record_id", ""),
            event.get("source_url", ""),
            event.get("signal_type", "generic_signal"),
            event.get("project_name_raw", ""),
            event.get("company_name_raw", ""),
            event.get("city", ""),
            event.get("state", ""),
            event.get("observed_at", ""),
            event.get("effective_date", ""),
            float(event.get("confidence_score", 0.0)),
            event.get("raw_payload_ref", ""),
            json.dumps(event.get("raw_payload", {})),
        ),
    )


def ingest_permit_signals(cur: sqlite3.Cursor, permits_path: Path) -> Tuple[int, int]:
    payload = load_json(permits_path)
    rows = payload.get("rows", []) if isinstance(payload, dict) else []
    run_id = start_run(cur, "permits_imported", "permit", notes=str(permits_path))
    seen = len(rows)
    ingested = 0

    for row in rows:
        permit_id = norm_text(row.get("permit_id"))
        source = norm_text(row.get("source")) or "permit_source"
        city = norm_text(row.get("city"))
        state = norm_text(row.get("state")).upper()
        description = norm_text(row.get("description"))
        source_url = norm_text(row.get("source_url"))
        issued_at = norm_text(row.get("issued_at"))

        item_key = hash_key("permit", source, permit_id or description, city, state)
        cur.execute(
            """
            INSERT INTO permit_signal_items (
                source_item_key, source_name, permit_id, description, address, city, state,
                issued_at, source_url, raw_payload
            ) VALUES (?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(source_item_key) DO UPDATE SET
                description=excluded.description,
                address=excluded.address,
                city=excluded.city,
                state=excluded.state,
                issued_at=excluded.issued_at,
                source_url=excluded.source_url,
                raw_payload=excluded.raw_payload
            """,
            (
                item_key,
                source,
                permit_id,
                description,
                norm_text(row.get("address")),
                city,
                state,
                issued_at,
                source_url,
                json.dumps(row),
            ),
        )

        signal_key = hash_key("signal", "permit", item_key)
        insert_signal_event(
            cur,
            {
                "signal_key": signal_key,
                "source_name": "permits_imported",
                "source_record_id": permit_id,
                "source_url": source_url,
                "signal_type": "permit_activity",
                "project_name_raw": description or (f"Permit {permit_id}" if permit_id else "Permit Project"),
                "company_name_raw": source,
                "city": city,
                "state": state,
                "observed_at": issued_at,
                "effective_date": issued_at,
                "confidence_score": 0.72,
                "raw_payload_ref": f"permit_signal_items:{item_key}",
                "raw_payload": row,
            },
        )
        ingested += 1

    finish_run(cur, run_id, seen, ingested)
    return seen, ingested


def ingest_job_signals(cur: sqlite3.Cursor, jobs_path: Path) -> Tuple[int, int]:
    payload = load_json(jobs_path)
    rows = payload.get("jobs", []) if isinstance(payload, dict) else []
    run_id = start_run(cur, "jobs_imported", "jobs", notes=str(jobs_path))
    seen = len(rows)
    ingested = 0

    for row in rows:
        title = norm_text(row.get("title"))
        company = norm_text(row.get("company"))
        location = norm_text(row.get("location"))
        state = ""
        if "," in location:
            parts = [p.strip() for p in location.split(",") if p.strip()]
            state = parts[-1].upper()[:2] if parts else ""
        source = norm_text(row.get("source")) or "jobs_source"
        source_url = norm_text(row.get("source_url"))

        item_key = hash_key("jobs", source, title, company, location)
        cur.execute(
            """
            INSERT INTO jobs_signal_items (
                source_item_key, source_name, job_title, company_name, location, city, state,
                employment_type, description, posted_at, source_url, raw_payload
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(source_item_key) DO UPDATE SET
                job_title=excluded.job_title,
                company_name=excluded.company_name,
                location=excluded.location,
                city=excluded.city,
                state=excluded.state,
                employment_type=excluded.employment_type,
                description=excluded.description,
                posted_at=excluded.posted_at,
                source_url=excluded.source_url,
                raw_payload=excluded.raw_payload
            """,
            (
                item_key,
                source,
                title,
                company,
                location,
                "",
                state,
                norm_text(row.get("type")),
                norm_text(row.get("description")),
                norm_text(row.get("posted_at")),
                source_url,
                json.dumps(row),
            ),
        )

        signal_key = hash_key("signal", "jobs", item_key)
        insert_signal_event(
            cur,
            {
                "signal_key": signal_key,
                "source_name": "jobs_imported",
                "source_record_id": item_key,
                "source_url": source_url,
                "signal_type": "hiring_signal",
                "project_name_raw": title,
                "company_name_raw": company,
                "city": "",
                "state": state,
                "observed_at": norm_text(row.get("posted_at")),
                "effective_date": norm_text(row.get("posted_at")),
                "confidence_score": 0.58 if "seed" in source else 0.68,
                "raw_payload_ref": f"jobs_signal_items:{item_key}",
                "raw_payload": row,
            },
        )
        ingested += 1

    finish_run(cur, run_id, seen, ingested)
    return seen, ingested


def ingest_db_signals(cur: sqlite3.Cursor) -> Tuple[int, int]:
    run_id = start_run(cur, "db_signals", "internal")
    cur.execute(
        """
        SELECT signal_id, signal_type, signal_value, signal_date, signal_confidence, source, source_url, company_id, project_id
        FROM signals
        """
    )
    rows = cur.fetchall()
    seen = len(rows)
    ingested = 0

    for r in rows:
        signal_id, signal_type, signal_value, signal_date, signal_conf, source, source_url, company_id, project_id = r
        company_name = ""
        city = ""
        state = ""
        project_name = norm_text(signal_value)

        if company_id:
            cur.execute("SELECT company_name, location_city, location_state FROM companies WHERE company_id=?", (company_id,))
            c = cur.fetchone()
            if c:
                company_name = norm_text(c[0])
                city = norm_text(c[1])
                state = norm_text(c[2]).upper()
        if project_id:
            cur.execute("SELECT project_name, location_city, location_state FROM projects WHERE project_id=?", (project_id,))
            p = cur.fetchone()
            if p:
                project_name = norm_text(p[0]) or project_name
                city = norm_text(p[1]) or city
                state = norm_text(p[2]).upper() or state

        signal_key = hash_key("signal", "db", str(signal_id), signal_type)
        insert_signal_event(
            cur,
            {
                "signal_key": signal_key,
                "source_name": norm_text(source) or "db_signals",
                "source_record_id": str(signal_id),
                "source_url": norm_text(source_url),
                "signal_type": norm_text(signal_type) or "db_signal",
                "project_name_raw": project_name or f"Signal {signal_id}",
                "company_name_raw": company_name,
                "city": city,
                "state": state,
                "observed_at": norm_text(signal_date),
                "effective_date": norm_text(signal_date),
                "confidence_score": float(signal_conf or 0.6),
                "raw_payload_ref": f"signals:{signal_id}",
                "raw_payload": {
                    "signal_id": signal_id,
                    "signal_type": signal_type,
                    "signal_value": signal_value,
                    "company_id": company_id,
                    "project_id": project_id,
                },
            },
        )
        ingested += 1

    finish_run(cur, run_id, seen, ingested)
    return seen, ingested


def classify_vertical(text: str) -> str:
    t = norm_key(text)
    for vertical, kws in VERTICAL_KEYWORDS.items():
        if any(kw in t for kw in kws):
            return vertical
    return "other"


def classify_project_type(text: str, signal_type: str) -> str:
    t = norm_key(text)
    if "permit" in signal_type:
        return "permit_driven_project"
    if "hiring" in signal_type:
        return "hiring_expansion_project"
    if "substation" in t or "utility" in t or "power" in t:
        return "power_project"
    if "bridge" in t or "transit" in t or "rail" in t or "highway" in t:
        return "major_infrastructure_project"
    if "warehouse" in t or "distribution" in t or "logistics" in t:
        return "logistics_project"
    if "hospital" in t:
        return "healthcare_project"
    if "data center" in t:
        return "data_center_project"
    if "industrial" in t or "plant" in t or "manufacturing" in t:
        return "industrial_project"
    return "general_project"


def score_spend(vertical: str, text: str, signal_count: int) -> float:
    score = float(SPEND_BASE.get(vertical, SPEND_BASE["other"]))
    t = norm_key(text)
    if any(k in t for k in ["phase ii", "phase 2", "expansion", "megaproject", "critical", "hyperscale"]):
        score += 6
    if signal_count >= 3:
        score += 4
    return max(0.0, min(100.0, round(score, 2)))


def score_crane_relevance(vertical: str, text: str) -> float:
    score = float(CRANE_RELEVANCE_BASE.get(vertical, CRANE_RELEVANCE_BASE["other"]))
    t = norm_key(text)
    if any(k in t for k in ["steel", "tower", "lift", "rigging", "precast", "module", "structural"]):
        score += 6
    return max(0.0, min(100.0, round(score, 2)))


def mini_keyword_hits(text: str) -> Tuple[List[str], List[str]]:
    t = norm_key(text)
    hits = [kw for kw in MINI_KEYWORDS if kw in t]
    strong = [kw for kw in STRONG_MINI_KEYWORDS if kw in t]
    return hits, strong


def score_mini_fit(text: str, vertical: str) -> float:
    hits, strong = mini_keyword_hits(text)
    if not hits:
        return 0.0
    score = 38.0 + 12.0 * len(strong) + 5.0 * max(0, len(hits) - len(strong))
    if vertical in {"data_centers", "healthcare_hospitals", "institutional_campus_stadium"}:
        score += 6
    return max(0.0, min(100.0, round(score, 2)))


def score_timing(latest_signal_date: str, signal_count: int) -> float:
    score = 52.0
    if latest_signal_date:
        score += 14
    if signal_count >= 2:
        score += 8
    if signal_count >= 4:
        score += 8
    return max(0.0, min(100.0, round(score, 2)))


def score_matchability(source_count: int, state: str, company_name: str) -> float:
    score = 45.0
    if source_count >= 2:
        score += 15
    if state:
        score += 15
    if company_name:
        score += 12
    return max(0.0, min(100.0, round(score, 2)))


def score_confidence(avg_signal_conf: float, source_count: int, signal_count: int) -> float:
    score = avg_signal_conf * 100.0
    if source_count >= 2:
        score += 10
    if signal_count >= 3:
        score += 8
    return max(0.0, min(100.0, round(score, 2)))


def score_monetization(spend: float, crane_rel: float, demand: float, timing: float, matchability: float, confidence: float) -> float:
    score = 0.27 * spend + 0.24 * crane_rel + 0.16 * demand + 0.11 * timing + 0.11 * matchability + 0.11 * confidence
    return round(max(0.0, min(100.0, score)), 2)


def candidate_key_from_event(project_name: str, company_name: str, city: str, state: str, signal_type: str) -> str:
    seed = project_name or company_name or signal_type
    return hash_key(seed, company_name, city, state)


def source_confidence_adjustment(sources: set[str], signal_types: List[str]) -> float:
    adj = 0.0
    for sname in sources:
        adj += SOURCE_CONFIDENCE_BONUS.get(sname, 0.0)
    if signal_types and all(st == "hiring_signal" for st in signal_types):
        adj -= 0.08
    if any(st == "permit_activity" for st in signal_types):
        adj += 0.05
    return adj


def extract_signal_text(raw_payload: str) -> str:
    if not raw_payload:
        return ""
    try:
        payload = json.loads(raw_payload)
    except Exception:
        return ""
    if not isinstance(payload, dict):
        return ""
    fields = [
        payload.get("description"),
        payload.get("title"),
        payload.get("permit_type"),
        payload.get("address"),
        payload.get("location"),
        payload.get("job_title"),
        payload.get("project_name"),
        payload.get("scope"),
        payload.get("work_type"),
    ]
    parts = [norm_text(v) for v in fields if norm_text(v)]
    return " ".join(parts).strip()


def build_candidates(cur: sqlite3.Cursor) -> int:
    cur.execute("DELETE FROM project_signal_links")

    cur.execute(
        """
        SELECT signal_event_id, source_name, signal_type, project_name_raw, company_name_raw,
               city, state, observed_at, effective_date, confidence_score, raw_payload
        FROM signal_events
        """
    )
    rows = cur.fetchall()

    grouped: Dict[str, Dict] = {}

    for r in rows:
        signal_event_id = int(r[0])
        source_name = norm_text(r[1])
        signal_type = norm_text(r[2])
        project_name = norm_text(r[3])
        company_name = norm_text(r[4])
        city = norm_text(r[5])
        state = norm_text(r[6]).upper()
        observed = norm_text(r[7])
        eff = norm_text(r[8])
        conf = float(r[9] or 0.0)
        raw_payload = norm_text(r[10])
        signal_text = extract_signal_text(raw_payload)

        key = candidate_key_from_event(project_name, company_name, city, state, signal_type)
        entry = grouped.setdefault(
            key,
            {
                "project_name_raw": project_name or signal_type,
                "company_name_raw": company_name,
                "city": city,
                "state": state,
                "sources": set(),
                "signals": [],
                "dates": [],
                "conf": [],
                "signal_ids": [],
                "signal_texts": [],
            },
        )

        entry["sources"].add(source_name)
        entry["signals"].append(signal_type)
        if observed:
            entry["dates"].append(observed)
        if eff:
            entry["dates"].append(eff)
        entry["conf"].append(conf)
        entry["signal_ids"].append(signal_event_id)
        if signal_text:
            entry["signal_texts"].append(signal_text)

    candidate_rows = []
    for key, e in grouped.items():
        context_text = " ".join(
            [e["project_name_raw"], e["company_name_raw"], " ".join(e["signals"]), " ".join(e.get("signal_texts", []))]
        )
        vertical = classify_vertical(context_text)
        project_type = classify_project_type(context_text, e["signals"][0] if e["signals"] else "")

        source_count = len(e["sources"])
        signal_count = len(e["signal_ids"])
        dates = sorted([d for d in e["dates"] if d])
        earliest = dates[0] if dates else ""
        latest = dates[-1] if dates else ""
        avg_conf = sum(e["conf"]) / len(e["conf"]) if e["conf"] else 0.5
        avg_conf = max(0.0, min(1.0, avg_conf + source_confidence_adjustment(e["sources"], e["signals"])))

        spend = score_spend(vertical, context_text, signal_count)
        crane_rel = score_crane_relevance(vertical, context_text)
        mini = score_mini_fit(context_text, vertical)

        # Quality guardrails: hiring-only weak signals should rank lower.
        if e["signals"] and all(st == "hiring_signal" for st in e["signals"]):
            spend = max(0.0, spend - 8)
            crane_rel = max(0.0, crane_rel - 6)

        demand = round(min(100.0, 0.58 * crane_rel + 0.42 * spend), 2)
        timing = score_timing(latest, signal_count)
        matchability = score_matchability(source_count, e["state"], e["company_name_raw"])
        confidence = score_confidence(avg_conf, source_count, signal_count)
        monetization = score_monetization(spend, crane_rel, demand, timing, matchability, confidence)

        candidate_rows.append(
            {
                "candidate_key": key,
                "project_name_raw": e["project_name_raw"],
                "project_name_normalized": norm_key(e["project_name_raw"]),
                "project_type": project_type,
                "vertical": vertical,
                "city": e["city"],
                "state": e["state"],
                "metro": f"{e['city']}, {e['state']}".strip(", "),
                "company_name_raw": e["company_name_raw"],
                "company_name_normalized": norm_key(e["company_name_raw"]),
                "source_count": source_count,
                "signal_count": signal_count,
                "earliest_signal_date": earliest,
                "latest_signal_date": latest,
                "estimated_spend_proxy": spend,
                "crane_relevance_score": crane_rel,
                "mini_crane_fit_score": mini,
                "demand_score": demand,
                "timing_score": timing,
                "matchability_score": matchability,
                "monetization_score": monetization,
                "confidence_score": confidence,
                "signals": list(e["signals"]),
                "sources": list(e["sources"]),
                "signal_ids": list(e["signal_ids"]),
            }
        )

    # Determine strong expansion states from where we have best monetization+confidence density.
    state_stats: Dict[str, Dict[str, float]] = {}
    for c in candidate_rows:
        st = c["state"]
        if not st:
            continue
        s = state_stats.setdefault(st, {"count": 0, "mon_sum": 0.0, "conf_sum": 0.0})
        s["count"] += 1
        s["mon_sum"] += c["monetization_score"]
        s["conf_sum"] += c["confidence_score"]

    state_rank = []
    for st, v in state_stats.items():
        avg_mon = v["mon_sum"] / max(1, v["count"])
        avg_conf = v["conf_sum"] / max(1, v["count"])
        strength = avg_mon * 0.6 + avg_conf * 0.4 + min(10, v["count"] * 1.5)
        state_rank.append((strength, st, v["count"], avg_mon, avg_conf))
    state_rank.sort(reverse=True)
    top_expansion_states = {st for _, st, cnt, _, _ in state_rank[:5] if cnt >= 2}

    upserts = 0
    for c in candidate_rows:
        mini_hits, strong_mini_hits = mini_keyword_hits(" ".join([c["project_name_raw"], c["company_name_raw"], c["project_type"]]))

        recommended = 0
        rec_reason = ""
        if c["state"] in top_expansion_states and c["monetization_score"] >= 72 and c["confidence_score"] >= 55:
            recommended = 1
            rec_reason = f"expansion_target_state:{c['state']} high_monetization:{round(c['monetization_score'],1)}"
        elif c["mini_crane_fit_score"] >= 70 and len(strong_mini_hits) >= 1 and c["confidence_score"] >= 52:
            recommended = 1
            rec_reason = f"mini_tight_access_signal:{strong_mini_hits[0]}"

        reasons = []
        if c["estimated_spend_proxy"] >= 85:
            reasons.append("high_spend_proxy")
        if c["crane_relevance_score"] >= 82:
            reasons.append("strong_crane_relevance")
        if c["source_count"] >= 2:
            reasons.append("multi_source")
        if c["signal_count"] >= 2:
            reasons.append("multi_signal")
        if c["mini_crane_fit_score"] >= 70 and strong_mini_hits:
            reasons.append(f"mini_fit:{strong_mini_hits[0]}")
        if not reasons and c["confidence_score"] >= 65:
            reasons.append("high_confidence")
        priority_reason = "; ".join(reasons)

        status = "active" if c["confidence_score"] >= 45 and c["monetization_score"] >= 60 else "low_confidence"

        cur.execute(
            """
            INSERT INTO project_candidates (
                candidate_key, project_name_raw, project_name_normalized, project_type, vertical,
                city, state, metro, company_name_raw, company_name_normalized,
                source_count, signal_count, earliest_signal_date, latest_signal_date,
                estimated_spend_proxy, crane_relevance_score, mini_crane_fit_score, demand_score,
                timing_score, matchability_score, monetization_score, confidence_score,
                recommended_flag, recommendation_reason, priority_reason, status
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(candidate_key) DO UPDATE SET
                project_name_raw=excluded.project_name_raw,
                project_name_normalized=excluded.project_name_normalized,
                project_type=excluded.project_type,
                vertical=excluded.vertical,
                city=excluded.city,
                state=excluded.state,
                metro=excluded.metro,
                company_name_raw=excluded.company_name_raw,
                company_name_normalized=excluded.company_name_normalized,
                source_count=excluded.source_count,
                signal_count=excluded.signal_count,
                earliest_signal_date=excluded.earliest_signal_date,
                latest_signal_date=excluded.latest_signal_date,
                estimated_spend_proxy=excluded.estimated_spend_proxy,
                crane_relevance_score=excluded.crane_relevance_score,
                mini_crane_fit_score=excluded.mini_crane_fit_score,
                demand_score=excluded.demand_score,
                timing_score=excluded.timing_score,
                matchability_score=excluded.matchability_score,
                monetization_score=excluded.monetization_score,
                confidence_score=excluded.confidence_score,
                recommended_flag=excluded.recommended_flag,
                recommendation_reason=excluded.recommendation_reason,
                priority_reason=excluded.priority_reason,
                status=excluded.status,
                updated_at=CURRENT_TIMESTAMP
            """,
            (
                c["candidate_key"],
                c["project_name_raw"],
                c["project_name_normalized"],
                c["project_type"],
                c["vertical"],
                c["city"],
                c["state"],
                c["metro"],
                c["company_name_raw"],
                c["company_name_normalized"],
                c["source_count"],
                c["signal_count"],
                c["earliest_signal_date"],
                c["latest_signal_date"],
                c["estimated_spend_proxy"],
                c["crane_relevance_score"],
                c["mini_crane_fit_score"],
                c["demand_score"],
                c["timing_score"],
                c["matchability_score"],
                c["monetization_score"],
                c["confidence_score"],
                recommended,
                rec_reason,
                priority_reason,
                status,
            ),
        )

        cur.execute("SELECT project_candidate_id FROM project_candidates WHERE candidate_key=?", (c["candidate_key"],))
        project_candidate_id = int(cur.fetchone()[0])

        for sid in c["signal_ids"]:
            cur.execute(
                """
                INSERT OR REPLACE INTO project_signal_links (
                    project_candidate_id, signal_event_id, link_strength, link_reason
                ) VALUES (?,?,?,?)
                """,
                (project_candidate_id, sid, min(1.0, round(c["confidence_score"] / 100.0, 3)), "deterministic_cluster_key"),
            )

        upserts += 1

    return upserts

def run(db_path: str, repo_root: Path) -> None:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    schema_file = repo_root / "contact_intelligence" / "schema" / "project_intelligence_extensions.sql"
    apply_schema(conn, schema_file)

    permits_path = repo_root / "data" / "opportunities" / "permits_imported.json"
    jobs_path = repo_root / "data" / "jobs_imported.json"

    p_seen, p_ing = ingest_permit_signals(cur, permits_path)
    j_seen, j_ing = ingest_job_signals(cur, jobs_path)
    s_seen, s_ing = ingest_db_signals(cur)

    candidates = build_candidates(cur)

    conn.commit()

    cur.execute("SELECT COUNT(*) FROM signal_events")
    total_signals = int(cur.fetchone()[0])
    cur.execute("SELECT COUNT(*) FROM project_candidates")
    total_candidates = int(cur.fetchone()[0])

    conn.close()

    print("[project-intel] build complete")
    print(f"[project-intel] permit rows seen/ingested: {p_seen}/{p_ing}")
    print(f"[project-intel] jobs rows seen/ingested: {j_seen}/{j_ing}")
    print(f"[project-intel] db signals seen/ingested: {s_seen}/{s_ing}")
    print(f"[project-intel] candidate upserts: {candidates}")
    print(f"[project-intel] total signal_events: {total_signals}")
    print(f"[project-intel] total project_candidates: {total_candidates}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Build Project Intelligence layer from available signals.")
    parser.add_argument("--db", default=None, help="Path to cranegenius_ci.db")
    parser.add_argument("--repo-root", default=None, help="Repo root path")
    args = parser.parse_args()

    db_path = args.db or os.environ.get("CRANEGENIUS_CI_DB", DEFAULT_DB)
    repo_root = Path(args.repo_root) if args.repo_root else Path(__file__).resolve().parents[2]

    if not Path(db_path).exists():
        raise SystemExit(f"DB not found: {db_path}")

    run(db_path, repo_root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
