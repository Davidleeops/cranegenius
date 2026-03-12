#!/usr/bin/env python3
"""
Normalize jobs/permit/manpower feeds into first-class CI tables and build match candidates.

Reads:
- data/jobs_imported.json
- data/opportunities/permits_imported.json
- operator_network + contacts (for manpower profiles)

Writes:
- jobs_feed_items, manpower_profiles, opportunity_feed_items
- job_contact_matches, opportunity_company_matches, manpower_job_matches
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

DEFAULT_DB = os.path.expanduser("~/data_runtime/cranegenius_ci.db")

ROLE_RE = {
    "operator": re.compile(r"\b(operator|hoist|mobile|crawler|tower)\b", re.IGNORECASE),
    "rigger": re.compile(r"\b(rigger|rigging|signal)\b", re.IGNORECASE),
    "supervisor": re.compile(r"\b(superintendent|supervisor|foreman|director|manager)\b", re.IGNORECASE),
    "apprentice": re.compile(r"\b(apprentice|trainee|helper)\b", re.IGNORECASE),
}
STATE_RE = re.compile(r"\b([A-Z]{2})\b")
COMPANY_STRIP = re.compile(r"\b(inc\.?|llc\.?|ltd\.?|corp\.?|co\.?|company|group|services?|solutions?|construction|contractors?)\b", re.IGNORECASE)


def norm_text(v: object) -> str:
    return str(v or "").strip()


def norm_company(v: str) -> str:
    x = COMPANY_STRIP.sub("", norm_text(v).lower())
    x = re.sub(r"[^a-z0-9\s]", "", x)
    return re.sub(r"\s+", " ", x).strip()


def parse_state(location_text: str) -> str:
    text = norm_text(location_text)
    if not text:
        return ""
    parts = [p.strip() for p in text.split(",") if p.strip()]
    if parts:
        last = parts[-1].upper()
        if len(last) == 2 and last.isalpha():
            return last
    m = STATE_RE.search(text.upper())
    return m.group(1) if m else ""


def role_hits(*values: str) -> Dict[str, int]:
    blob = " ".join(norm_text(v) for v in values)
    return {k: int(bool(rx.search(blob))) for k, rx in ROLE_RE.items()}


def ensure_phase2_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS jobs_feed_items (
            job_feed_id            INTEGER PRIMARY KEY AUTOINCREMENT,
            external_key           TEXT NOT NULL UNIQUE,
            source                 TEXT,
            source_url             TEXT,
            title                  TEXT NOT NULL,
            company_name           TEXT,
            normalized_company_name TEXT,
            location_text          TEXT,
            location_state         TEXT,
            employment_type        TEXT,
            description            TEXT,
            posted_at              TEXT,
            record_quality_score   REAL DEFAULT 0.0,
            created_at             DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at             DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS manpower_profiles (
            manpower_profile_id    INTEGER PRIMARY KEY AUTOINCREMENT,
            external_key           TEXT NOT NULL UNIQUE,
            source                 TEXT,
            source_url             TEXT,
            full_name              TEXT,
            normalized_full_name   TEXT,
            title                  TEXT,
            certifications         TEXT,
            location_state         TEXT,
            availability_status    TEXT,
            email                  TEXT,
            phone                  TEXT,
            record_quality_score   REAL DEFAULT 0.0,
            created_at             DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at             DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS opportunity_feed_items (
            opportunity_feed_id    INTEGER PRIMARY KEY AUTOINCREMENT,
            external_key           TEXT NOT NULL UNIQUE,
            source                 TEXT,
            source_url             TEXT,
            project_name           TEXT,
            normalized_project_name TEXT,
            city                   TEXT,
            location_state         TEXT,
            opportunity_type       TEXT,
            project_stage          TEXT,
            summary                TEXT,
            captured_at            TEXT,
            record_quality_score   REAL DEFAULT 0.0,
            created_at             DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at             DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS job_contact_matches (
            job_contact_match_id   INTEGER PRIMARY KEY AUTOINCREMENT,
            job_feed_id            INTEGER NOT NULL REFERENCES jobs_feed_items(job_feed_id),
            contact_id             INTEGER NOT NULL REFERENCES contacts(contact_id),
            company_id             INTEGER REFERENCES companies(company_id),
            match_score            REAL DEFAULT 0.0,
            match_reason           TEXT,
            created_at             DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(job_feed_id, contact_id)
        );

        CREATE TABLE IF NOT EXISTS opportunity_company_matches (
            opportunity_company_match_id INTEGER PRIMARY KEY AUTOINCREMENT,
            opportunity_feed_id     INTEGER NOT NULL REFERENCES opportunity_feed_items(opportunity_feed_id),
            company_id              INTEGER NOT NULL REFERENCES companies(company_id),
            match_score             REAL DEFAULT 0.0,
            match_reason            TEXT,
            created_at              DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(opportunity_feed_id, company_id)
        );

        CREATE TABLE IF NOT EXISTS manpower_job_matches (
            manpower_job_match_id   INTEGER PRIMARY KEY AUTOINCREMENT,
            manpower_profile_id     INTEGER NOT NULL REFERENCES manpower_profiles(manpower_profile_id),
            job_feed_id             INTEGER NOT NULL REFERENCES jobs_feed_items(job_feed_id),
            match_score             REAL DEFAULT 0.0,
            match_reason            TEXT,
            created_at              DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(manpower_profile_id, job_feed_id)
        );

        CREATE INDEX IF NOT EXISTS idx_jfi_state           ON jobs_feed_items(location_state);
        CREATE INDEX IF NOT EXISTS idx_jfi_norm_company    ON jobs_feed_items(normalized_company_name);
        CREATE INDEX IF NOT EXISTS idx_mp_state            ON manpower_profiles(location_state);
        CREATE INDEX IF NOT EXISTS idx_ofi_state           ON opportunity_feed_items(location_state);
        CREATE INDEX IF NOT EXISTS idx_jcm_job             ON job_contact_matches(job_feed_id);
        CREATE INDEX IF NOT EXISTS idx_ocm_opp             ON opportunity_company_matches(opportunity_feed_id);
        CREATE INDEX IF NOT EXISTS idx_mjm_profile         ON manpower_job_matches(manpower_profile_id);
        """
    )
    conn.commit()


def load_json(path: Path) -> Dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def upsert_jobs(cur: sqlite3.Cursor, jobs_payload: Dict) -> int:
    jobs = jobs_payload.get("jobs") if isinstance(jobs_payload, dict) else []
    if not isinstance(jobs, list):
        jobs = []
    count = 0
    for row in jobs:
        title = norm_text(row.get("title"))
        company = norm_text(row.get("company"))
        if not title or not company:
            continue
        location = norm_text(row.get("location"))
        state = parse_state(location)
        description = norm_text(row.get("description"))
        source = norm_text(row.get("source") or "jobs_import")
        source_url = norm_text(row.get("source_url"))
        posted_at = norm_text(row.get("posted_at"))
        ext = f"{source}|{title.lower()}|{company.lower()}|{location.lower()}"
        quality = 0.45
        if source_url:
            quality += 0.15
        if len(description) >= 60:
            quality += 0.20
        if state:
            quality += 0.10
        if "seed" in source:
            quality -= 0.08
        quality = max(0.0, min(1.0, round(quality, 3)))

        cur.execute(
            """
            INSERT INTO jobs_feed_items (
                external_key, source, source_url, title, company_name, normalized_company_name,
                location_text, location_state, employment_type, description, posted_at, record_quality_score
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(external_key) DO UPDATE SET
                source=excluded.source,
                source_url=excluded.source_url,
                title=excluded.title,
                company_name=excluded.company_name,
                normalized_company_name=excluded.normalized_company_name,
                location_text=excluded.location_text,
                location_state=excluded.location_state,
                employment_type=excluded.employment_type,
                description=excluded.description,
                posted_at=excluded.posted_at,
                record_quality_score=excluded.record_quality_score,
                updated_at=CURRENT_TIMESTAMP
            """,
            (
                ext,
                source,
                source_url,
                title,
                company,
                norm_company(company),
                location,
                state,
                norm_text(row.get("type")) or "Full-Time",
                description,
                posted_at,
                quality,
            ),
        )
        count += 1
    return count


def upsert_opportunities(cur: sqlite3.Cursor, permits_payload: Dict) -> int:
    rows = permits_payload.get("rows") if isinstance(permits_payload, dict) else []
    if not isinstance(rows, list):
        rows = []
    count = 0
    for row in rows:
        source = norm_text(row.get("source") or "permit_import")
        permit_id = norm_text(row.get("permit_id"))
        city = norm_text(row.get("city"))
        state = norm_text(row.get("state")).upper()
        desc = norm_text(row.get("description"))
        addr = norm_text(row.get("address"))
        project_name = desc if desc else (f"Permit {permit_id}" if permit_id else "Permit Opportunity")
        ext = f"{source}|{permit_id or project_name.lower()}|{city.lower()}|{state}"
        summary = " | ".join([x for x in [desc, addr] if x])
        quality = 0.48
        if permit_id:
            quality += 0.18
        if state:
            quality += 0.10
        if len(desc) >= 30:
            quality += 0.12
        quality = max(0.0, min(1.0, round(quality, 3)))

        cur.execute(
            """
            INSERT INTO opportunity_feed_items (
                external_key, source, source_url, project_name, normalized_project_name,
                city, location_state, opportunity_type, project_stage, summary, captured_at, record_quality_score
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(external_key) DO UPDATE SET
                source=excluded.source,
                source_url=excluded.source_url,
                project_name=excluded.project_name,
                normalized_project_name=excluded.normalized_project_name,
                city=excluded.city,
                location_state=excluded.location_state,
                opportunity_type=excluded.opportunity_type,
                project_stage=excluded.project_stage,
                summary=excluded.summary,
                captured_at=excluded.captured_at,
                record_quality_score=excluded.record_quality_score,
                updated_at=CURRENT_TIMESTAMP
            """,
            (
                ext,
                source,
                norm_text(row.get("source_url")),
                project_name,
                norm_company(project_name),
                city,
                state,
                "permit_opportunity",
                "planning",
                summary,
                norm_text(row.get("issued_at")),
                quality,
            ),
        )
        count += 1
    return count


def upsert_manpower_profiles(cur: sqlite3.Cursor) -> int:
    count = 0
    cur.execute(
        """
        SELECT operator_id, full_name, certifications, crane_types, location_state,
               availability_status, email, phone
        FROM operator_network
        """
    )
    for r in cur.fetchall():
        ext = f"operator_network|{r[0]}"
        full_name = norm_text(r[1])
        certs = norm_text(r[2])
        title = norm_text(r[3])
        state = norm_text(r[4]).upper()
        quality = 0.58
        if certs:
            quality += 0.15
        if state:
            quality += 0.10
        if r[6]:
            quality += 0.08
        cur.execute(
            """
            INSERT INTO manpower_profiles (
                external_key, source, source_url, full_name, normalized_full_name, title, certifications,
                location_state, availability_status, email, phone, record_quality_score
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(external_key) DO UPDATE SET
                source=excluded.source,
                full_name=excluded.full_name,
                normalized_full_name=excluded.normalized_full_name,
                title=excluded.title,
                certifications=excluded.certifications,
                location_state=excluded.location_state,
                availability_status=excluded.availability_status,
                email=excluded.email,
                phone=excluded.phone,
                record_quality_score=excluded.record_quality_score,
                updated_at=CURRENT_TIMESTAMP
            """,
            (
                ext,
                "operator_network",
                "",
                full_name,
                norm_company(full_name),
                title,
                certs,
                state,
                norm_text(r[5]),
                norm_text(r[6]),
                norm_text(r[7]),
                round(min(1.0, quality), 3),
            ),
        )
        count += 1

    cur.execute(
        """
        SELECT c.contact_id, c.full_name, c.title, c.location_state, c.email, c.phone,
               COALESCE(MAX(csf.record_quality_score), c.confidence_score, 0.4) AS quality
        FROM contacts c
        LEFT JOIN contact_source_facts csf ON csf.contact_id = c.contact_id
        WHERE lower(COALESCE(c.title,'')) REGEXP 'operator|rigger|signal|superintendent|foreman|hoist'
        GROUP BY c.contact_id
        """
    )
    for r in cur.fetchall():
        ext = f"contact|{r[0]}"
        cur.execute(
            """
            INSERT INTO manpower_profiles (
                external_key, source, source_url, full_name, normalized_full_name, title, certifications,
                location_state, availability_status, email, phone, record_quality_score
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(external_key) DO UPDATE SET
                full_name=excluded.full_name,
                normalized_full_name=excluded.normalized_full_name,
                title=excluded.title,
                location_state=excluded.location_state,
                email=excluded.email,
                phone=excluded.phone,
                record_quality_score=excluded.record_quality_score,
                updated_at=CURRENT_TIMESTAMP
            """,
            (
                ext,
                "contacts_inferred",
                "",
                norm_text(r[1]),
                norm_company(norm_text(r[1])),
                norm_text(r[2]),
                "",
                norm_text(r[3]).upper(),
                "unknown",
                norm_text(r[4]),
                norm_text(r[5]),
                round(min(1.0, float(r[6] or 0.4)), 3),
            ),
        )
        count += 1

    return count


def _contact_candidates(cur: sqlite3.Cursor) -> List[sqlite3.Row]:
    cur.execute(
        """
        WITH latest AS (
            SELECT f.*
            FROM contact_source_facts f
            JOIN (
                SELECT contact_id, MAX(last_seen_at) AS max_seen
                FROM contact_source_facts
                GROUP BY contact_id
            ) m ON m.contact_id=f.contact_id AND m.max_seen=f.last_seen_at
        )
        SELECT c.contact_id, c.company_id, c.full_name, c.title, c.contact_role, c.location_state,
               co.company_name, co.normalized_company_name, co.location_state AS company_state,
               COALESCE(l.record_quality_score, c.confidence_score, 0.45) AS quality,
               COALESCE(l.usable_for_outreach, CASE WHEN c.email_verified=1 THEN 1 ELSE 0 END) AS usable
        FROM contacts c
        LEFT JOIN companies co ON co.company_id=c.company_id
        LEFT JOIN latest l ON l.contact_id=c.contact_id
        """
    )
    return cur.fetchall()


def build_job_contact_matches(cur: sqlite3.Cursor) -> int:
    cur.execute("DELETE FROM job_contact_matches")
    cur.execute("SELECT job_feed_id, title, company_name, normalized_company_name, location_state, description FROM jobs_feed_items")
    jobs = cur.fetchall()
    contacts = _contact_candidates(cur)

    inserted = 0
    for j in jobs:
        j_roles = role_hits(j[1], j[5])
        j_state = norm_text(j[4]).upper()
        j_co_norm = norm_text(j[3])
        scored: List[Tuple[float, int, int | None, str]] = []
        for c in contacts:
            if int(c[10] or 0) == 0:
                continue
            score = 0.0
            reasons = []
            c_state = norm_text(c[5] or c[8]).upper()
            if j_state and c_state and j_state == c_state:
                score += 0.35
                reasons.append("state")
            c_roles = role_hits(c[3], c[4])
            overlap = sum(1 for k in j_roles if j_roles[k] and c_roles[k])
            if overlap:
                score += min(0.30, 0.15 * overlap)
                reasons.append("role")
            if j_co_norm and j_co_norm and j_co_norm == norm_text(c[7]):
                score += 0.20
                reasons.append("company")
            q = float(c[9] or 0.0)
            if q >= 0.70:
                score += 0.15
                reasons.append("quality")
            elif q >= 0.60:
                score += 0.08
            if score >= 0.50:
                scored.append((round(min(score, 1.0), 3), int(c[0]), c[1], "+".join(reasons)))

        scored.sort(key=lambda x: x[0], reverse=True)
        for s in scored[:15]:
            cur.execute(
                """
                INSERT OR REPLACE INTO job_contact_matches (job_feed_id, contact_id, company_id, match_score, match_reason)
                VALUES (?,?,?,?,?)
                """,
                (j[0], s[1], s[2], s[0], s[3]),
            )
            inserted += 1
    return inserted


def build_opportunity_company_matches(cur: sqlite3.Cursor) -> int:
    cur.execute("DELETE FROM opportunity_company_matches")
    cur.execute("SELECT opportunity_feed_id, location_state, summary, project_name FROM opportunity_feed_items")
    opps = cur.fetchall()
    cur.execute(
        """
        SELECT company_id, company_name, normalized_company_name, location_state, company_type, industry, domain, target_score
        FROM companies
        """
    )
    companies = cur.fetchall()

    inserted = 0
    for o in opps:
        state = norm_text(o[1]).upper()
        text = f"{norm_text(o[2])} {norm_text(o[3])}".lower()
        scored: List[Tuple[float, int, str]] = []
        for c in companies:
            score = 0.0
            reasons = []
            c_state = norm_text(c[3]).upper()
            if state and c_state and state == c_state:
                score += 0.40
                reasons.append("state")
            c_type = norm_text(c[4]).lower()
            c_ind = norm_text(c[5]).lower()
            if any(k in c_type or k in c_ind for k in ["crane", "lift", "rig", "construction", "industrial", "contractor"]):
                score += 0.25
                reasons.append("industry")
            if norm_text(c[6]):
                score += 0.20
                reasons.append("domain")
            if float(c[7] or 0.0) > 0:
                score += 0.10
                reasons.append("target")
            if any(k in text for k in ["steel", "industrial", "tower", "crane", "plant", "substation"]):
                score += 0.08
            if score >= 0.45:
                scored.append((round(min(score, 1.0), 3), int(c[0]), "+".join(reasons)))

        scored.sort(key=lambda x: x[0], reverse=True)
        for s in scored[:20]:
            cur.execute(
                """
                INSERT OR REPLACE INTO opportunity_company_matches (opportunity_feed_id, company_id, match_score, match_reason)
                VALUES (?,?,?,?)
                """,
                (o[0], s[1], s[0], s[2]),
            )
            inserted += 1
    return inserted


def build_manpower_job_matches(cur: sqlite3.Cursor) -> int:
    cur.execute("DELETE FROM manpower_job_matches")
    cur.execute("SELECT manpower_profile_id, title, certifications, location_state, record_quality_score FROM manpower_profiles")
    profiles = cur.fetchall()
    cur.execute("SELECT job_feed_id, title, description, location_state FROM jobs_feed_items")
    jobs = cur.fetchall()

    inserted = 0
    for p in profiles:
        p_roles = role_hits(p[1], p[2])
        p_state = norm_text(p[3]).upper()
        p_quality = float(p[4] or 0.0)
        scored: List[Tuple[float, int, str]] = []
        for j in jobs:
            score = 0.0
            reasons = []
            j_state = norm_text(j[3]).upper()
            if p_state and j_state and p_state == j_state:
                score += 0.40
                reasons.append("state")
            j_roles = role_hits(j[1], j[2])
            overlap = sum(1 for k in p_roles if p_roles[k] and j_roles[k])
            if overlap:
                score += min(0.35, 0.18 * overlap)
                reasons.append("role")
            if "ncco" in norm_text(p[2]).lower() and "ncco" in f"{norm_text(j[1])} {norm_text(j[2])}".lower():
                score += 0.12
                reasons.append("cert")
            if p_quality >= 0.70:
                score += 0.10
            if score >= 0.45:
                scored.append((round(min(score, 1.0), 3), int(j[0]), "+".join(reasons)))

        scored.sort(key=lambda x: x[0], reverse=True)
        for s in scored[:15]:
            cur.execute(
                """
                INSERT OR REPLACE INTO manpower_job_matches (manpower_profile_id, job_feed_id, match_score, match_reason)
                VALUES (?,?,?,?)
                """,
                (p[0], s[1], s[0], s[2]),
            )
            inserted += 1
    return inserted


def register_regexp(conn: sqlite3.Connection) -> None:
    def _regexp(pattern: str, value: str) -> int:
        if value is None:
            return 0
        return 1 if re.search(pattern, str(value), re.IGNORECASE) else 0

    conn.create_function("REGEXP", 2, _regexp)


def run(db_path: str, jobs_path: Path, permits_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    register_regexp(conn)
    ensure_phase2_tables(conn)
    cur = conn.cursor()

    jobs_payload = load_json(jobs_path)
    permits_payload = load_json(permits_path)

    jobs_n = upsert_jobs(cur, jobs_payload)
    opps_n = upsert_opportunities(cur, permits_payload)
    manpower_n = upsert_manpower_profiles(cur)

    jcm_n = build_job_contact_matches(cur)
    ocm_n = build_opportunity_company_matches(cur)
    mjm_n = build_manpower_job_matches(cur)

    conn.commit()
    conn.close()

    print("[phase2] feed normalization + matching complete")
    print(f"[phase2] jobs_feed_items upserted: {jobs_n}")
    print(f"[phase2] opportunity_feed_items upserted: {opps_n}")
    print(f"[phase2] manpower_profiles upserted: {manpower_n}")
    print(f"[phase2] job_contact_matches: {jcm_n}")
    print(f"[phase2] opportunity_company_matches: {ocm_n}")
    print(f"[phase2] manpower_job_matches: {mjm_n}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Normalize feeds and build CI match candidates.")
    parser.add_argument("--db", default=None)
    parser.add_argument("--jobs", default=None, help="Path to jobs_imported.json")
    parser.add_argument("--permits", default=None, help="Path to permits_imported.json")
    args = parser.parse_args()

    db_path = args.db or os.environ.get("CRANEGENIUS_CI_DB", DEFAULT_DB)
    repo_root = Path(__file__).resolve().parents[2]
    jobs_path = Path(args.jobs) if args.jobs else (repo_root / "data" / "jobs_imported.json")
    permits_path = Path(args.permits) if args.permits else (repo_root / "data" / "opportunities" / "permits_imported.json")

    if not Path(db_path).exists():
        raise SystemExit(f"DB not found: {db_path}")

    run(db_path, jobs_path, permits_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
