#!/usr/bin/env python3
"""
contact_intelligence/scripts/normalize_records.py
Normalize source_records -> companies + contacts master tables and source-quality facts.

Idempotent for core entities and additive for provenance/quality tracking.
"""

import os
import re
import sys
import json
import sqlite3
import argparse
from pathlib import Path

DEFAULT_DB = os.path.expanduser("~/data_runtime/cranegenius_ci.db")

STRIP_SUFFIX = re.compile(
    r"\b(inc\.?|llc\.?|ltd\.?|corp\.?|co\.?|company|group|holdings?|"
    r"services?|solutions?|contractors?|construction|enterprises?|"
    r"associates?|international|national|global|partners?)\b",
    re.IGNORECASE,
)
PHONE_STRIP = re.compile(r"[^\d+]")

TITLE_MAP = {
    "project mgr": "project manager",
    "proj. manager": "project manager",
    " pm ": " project manager ",
    "supt": "superintendent",
    "vp ": "vice president ",
    "ceo": "chief executive officer",
    "cfo": "chief financial officer",
    "coo": "chief operating officer",
}

ROLE_MAP = {
    "project manager": "pm",
    "superintendent": "operations",
    "estimator": "estimator",
    "procurement": "procurement",
    "fleet manager": "operations",
    "rental manager": "operations",
    "branch manager": "operations",
    "dispatcher": "operations",
    "equipment manager": "operations",
    "vice president": "executive",
    "chief executive": "executive",
    "chief operating": "executive",
    "director": "director",
    "president": "executive",
}

REGION_MAP = {
    "TX": "Southwest",
    "OK": "Southwest",
    "NM": "Southwest",
    "AZ": "Southwest",
    "CO": "Southwest",
    "CA": "West",
    "OR": "West",
    "WA": "West",
    "NV": "West",
    "ID": "West",
    "UT": "West",
    "IL": "Midwest",
    "OH": "Midwest",
    "MI": "Midwest",
    "IN": "Midwest",
    "WI": "Midwest",
    "MN": "Midwest",
    "IA": "Midwest",
    "MO": "Midwest",
    "KS": "Midwest",
    "NE": "Midwest",
    "NY": "Northeast",
    "PA": "Northeast",
    "NJ": "Northeast",
    "MA": "Northeast",
    "CT": "Northeast",
    "FL": "Southeast",
    "GA": "Southeast",
    "NC": "Southeast",
    "SC": "Southeast",
    "TN": "Southeast",
    "AL": "Southeast",
    "MS": "Southeast",
    "LA": "Southeast",
    "VA": "Southeast",
    "KY": "Southeast",
}

JURISDICTION_STATE_MAP = {
    "chicago": "IL",
    "cook county": "IL",
    "dallas": "TX",
    "houston": "TX",
    "nyc": "NY",
    "new york": "NY",
    "manhattan": "NY",
    "brooklyn": "NY",
    "los angeles": "CA",
    "la": "CA",
    "phoenix": "AZ",
    "denver": "CO",
    "austin": "TX",
    "san antonio": "TX",
    "fort worth": "TX",
}

FREE_OR_ISP_DOMAIN_HINTS = (
    "gmail.com",
    "googlemail.com",
    "yahoo.com",
    "hotmail.com",
    "outlook.com",
    "icloud.com",
    "aol.com",
    "protonmail.com",
    "pm.me",
    "att.net",
    "comcast.net",
    "verizon.net",
)


def norm_company(name):
    if not name:
        return ""
    n = STRIP_SUFFIX.sub("", name.strip().lower())
    n = re.sub(r"[^\w\s]", "", n)
    return re.sub(r"\s+", " ", n).strip()


def norm_email(e):
    return e.strip().lower() if e else ""


def norm_phone(p):
    if not p:
        return ""
    d = PHONE_STRIP.sub("", p)
    if len(d) == 10:
        return f"({d[:3]}) {d[3:6]}-{d[6:]}"
    if len(d) == 11 and d[0] == "1":
        return f"({d[1:4]}) {d[4:7]}-{d[7:]}"
    return p.strip()


def split_name(full):
    if not full:
        return "", ""
    parts = full.strip().split()
    return (parts[0], " ".join(parts[1:])) if len(parts) > 1 else (parts[0], "")


def norm_title(t):
    if not t:
        return ""
    t = t.strip().lower()
    for k, v in TITLE_MAP.items():
        t = t.replace(k, v)
    return t.strip()


def infer_role(title):
    if not title:
        return None
    tl = title.lower()
    for kw, role in ROLE_MAP.items():
        if kw in tl:
            return role
    return None


def infer_region(state):
    return REGION_MAP.get((state or "").upper().strip())


def jurisdiction_to_state(jur):
    if not jur:
        return None
    j = jur.strip().lower()
    if len(j) == 2 and j.upper() in REGION_MAP:
        return j.upper()
    for k, v in JURISDICTION_STATE_MAP.items():
        if k in j:
            return v
    return None


def is_pipeline_row(raw):
    return (
        "contractor_name_normalized" in raw
        or "contractor_domain" in raw
        or "email_verification_status" in raw
        or "email_candidate" in raw
    )


def get_db(arg=None):
    return arg or os.environ.get("CRANEGENIUS_CI_DB", DEFAULT_DB)


def _email_domain(email: str) -> str:
    value = norm_email(email)
    if "@" not in value:
        return ""
    return value.rsplit("@", 1)[1].strip().lower()


def _email_domain_type(email: str) -> str:
    domain = _email_domain(email)
    if not domain:
        return "unknown"
    if any(domain.endswith(h) for h in FREE_OR_ISP_DOMAIN_HINTS):
        return "free_or_isp"
    return "corporate"


def _title_confidence(title: str) -> float:
    t = (title or "").lower()
    if any(x in t for x in ["chief", "ceo", "cfo", "coo", "president", "owner", "founder", "principal"]):
        return 0.90
    if any(x in t for x in ["vice president", "vp", "director"]):
        return 0.82
    if any(x in t for x in ["project manager", "superintendent", "estimator", "procurement", "operations manager"]):
        return 0.72
    if "manager" in t:
        return 0.65
    if t:
        return 0.55
    return 0.35


def _person_confidence(first: str, last: str, email: str, title: str, base_conf: float = 0.0) -> float:
    score = float(base_conf or 0.0)
    if first and last:
        score = max(score, 0.55)
    if email:
        score += 0.15
    if title:
        score += 0.10
    return round(min(score, 1.0), 3)


def _company_confidence(norm_company_name: str, domain: str) -> float:
    score = 0.0
    if norm_company_name:
        score += 0.50
    if domain:
        score += 0.35
    return round(min(score, 1.0), 3)


def _record_quality_score(person_conf: float, company_conf: float, title_conf: float, email_domain_type: str, verification_status: str) -> float:
    score = 0.40 * person_conf + 0.35 * company_conf + 0.15 * title_conf
    if verification_status == "valid":
        score += 0.10
    elif verification_status == "catchall":
        score += 0.04
    if email_domain_type == "free_or_isp":
        score -= 0.12
    return round(max(0.0, min(1.0, score)), 3)


def _usability(record_quality_score: float, email: str, email_domain_type: str) -> tuple[int, str, str]:
    if not email:
        return 0, "", "missing_email"
    if email_domain_type == "free_or_isp":
        return 0, "", "free_or_isp_email"
    if record_quality_score < 0.62:
        return 0, "", "low_quality_score"
    return 1, "quality_threshold_met", ""


def _ensure_extensions(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()

    cur.execute("PRAGMA table_info(source_records)")
    existing = {row[1] for row in cur.fetchall()}
    if "source_file" not in existing:
        cur.execute("ALTER TABLE source_records ADD COLUMN source_file TEXT")
    if "source_sheet" not in existing:
        cur.execute("ALTER TABLE source_records ADD COLUMN source_sheet TEXT")
    if "source_row" not in existing:
        cur.execute("ALTER TABLE source_records ADD COLUMN source_row INTEGER")

    cur.executescript(
        """
        CREATE TABLE IF NOT EXISTS contact_source_facts (
            contact_source_fact_id    INTEGER PRIMARY KEY AUTOINCREMENT,
            contact_id                INTEGER NOT NULL REFERENCES contacts(contact_id),
            company_id                INTEGER REFERENCES companies(company_id),
            source_record_id          INTEGER REFERENCES source_records(source_record_id),
            source_system             TEXT,
            source_file               TEXT,
            source_sheet              TEXT,
            source_row                INTEGER,
            ingested_at               DATETIME DEFAULT CURRENT_TIMESTAMP,
            first_seen_at             DATETIME DEFAULT CURRENT_TIMESTAMP,
            last_seen_at              DATETIME DEFAULT CURRENT_TIMESTAMP,
            email_verification_status TEXT,
            email_domain_type         TEXT,
            title_confidence          REAL DEFAULT 0.0,
            person_confidence         REAL DEFAULT 0.0,
            company_confidence        REAL DEFAULT 0.0,
            record_quality_score      REAL DEFAULT 0.0,
            usable_for_outreach       INTEGER DEFAULT 0,
            usable_reason             TEXT,
            blocked_reason            TEXT,
            notes                     TEXT,
            UNIQUE(contact_id, source_system, source_file, source_sheet, source_row)
        );

        CREATE TABLE IF NOT EXISTS company_source_facts (
            company_source_fact_id    INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id                INTEGER NOT NULL REFERENCES companies(company_id),
            source_record_id          INTEGER REFERENCES source_records(source_record_id),
            source_system             TEXT,
            source_file               TEXT,
            source_sheet              TEXT,
            source_row                INTEGER,
            ingested_at               DATETIME DEFAULT CURRENT_TIMESTAMP,
            first_seen_at             DATETIME DEFAULT CURRENT_TIMESTAMP,
            last_seen_at              DATETIME DEFAULT CURRENT_TIMESTAMP,
            source_support_count      INTEGER DEFAULT 1,
            preferred_domain          TEXT,
            domain_confidence         REAL DEFAULT 0.0,
            domain_evidence_notes     TEXT,
            quality_notes             TEXT,
            notes                     TEXT,
            UNIQUE(company_id, source_system, preferred_domain)
        );

        CREATE INDEX IF NOT EXISTS idx_csf_contact       ON contact_source_facts(contact_id);
        CREATE INDEX IF NOT EXISTS idx_csf_source_system ON contact_source_facts(source_system);
        CREATE INDEX IF NOT EXISTS idx_csf_usable        ON contact_source_facts(usable_for_outreach);
        CREATE INDEX IF NOT EXISTS idx_cof_company       ON company_source_facts(company_id);
        CREATE INDEX IF NOT EXISTS idx_cof_source_system ON company_source_facts(source_system);
        """
    )
    conn.commit()


def _upsert_company_fact(
    cur: sqlite3.Cursor,
    *,
    company_id: int,
    source_record_id: int,
    source_system: str,
    source_file: str,
    source_sheet: str,
    source_row: int | None,
    preferred_domain: str,
    domain_confidence: float,
    domain_evidence_notes: str,
    quality_notes: str,
) -> None:
    cur.execute(
        """
        INSERT INTO company_source_facts (
            company_id, source_record_id, source_system, source_file, source_sheet, source_row,
            source_support_count, preferred_domain, domain_confidence, domain_evidence_notes, quality_notes
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(company_id, source_system, preferred_domain) DO UPDATE SET
            source_support_count = company_source_facts.source_support_count + 1,
            source_record_id = excluded.source_record_id,
            source_file = COALESCE(excluded.source_file, company_source_facts.source_file),
            source_sheet = COALESCE(excluded.source_sheet, company_source_facts.source_sheet),
            source_row = COALESCE(excluded.source_row, company_source_facts.source_row),
            domain_confidence = MAX(company_source_facts.domain_confidence, excluded.domain_confidence),
            domain_evidence_notes = COALESCE(excluded.domain_evidence_notes, company_source_facts.domain_evidence_notes),
            quality_notes = COALESCE(excluded.quality_notes, company_source_facts.quality_notes),
            last_seen_at = CURRENT_TIMESTAMP
        """,
        (
            company_id,
            source_record_id,
            source_system,
            source_file or None,
            source_sheet or None,
            source_row,
            1,
            preferred_domain or "",
            float(domain_confidence or 0.0),
            domain_evidence_notes or None,
            quality_notes or None,
        ),
    )


def _upsert_contact_fact(
    cur: sqlite3.Cursor,
    *,
    contact_id: int,
    company_id: int | None,
    source_record_id: int,
    source_system: str,
    source_file: str,
    source_sheet: str,
    source_row: int | None,
    email_verification_status: str,
    email_domain_type: str,
    title_confidence: float,
    person_confidence: float,
    company_confidence: float,
    record_quality_score: float,
    usable_for_outreach: int,
    usable_reason: str,
    blocked_reason: str,
    notes: str,
) -> None:
    cur.execute(
        """
        INSERT INTO contact_source_facts (
            contact_id, company_id, source_record_id, source_system, source_file, source_sheet, source_row,
            email_verification_status, email_domain_type,
            title_confidence, person_confidence, company_confidence, record_quality_score,
            usable_for_outreach, usable_reason, blocked_reason, notes
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(contact_id, source_system, source_file, source_sheet, source_row) DO UPDATE SET
            company_id = COALESCE(excluded.company_id, contact_source_facts.company_id),
            source_record_id = excluded.source_record_id,
            email_verification_status = COALESCE(excluded.email_verification_status, contact_source_facts.email_verification_status),
            email_domain_type = COALESCE(excluded.email_domain_type, contact_source_facts.email_domain_type),
            title_confidence = MAX(contact_source_facts.title_confidence, excluded.title_confidence),
            person_confidence = MAX(contact_source_facts.person_confidence, excluded.person_confidence),
            company_confidence = MAX(contact_source_facts.company_confidence, excluded.company_confidence),
            record_quality_score = MAX(contact_source_facts.record_quality_score, excluded.record_quality_score),
            usable_for_outreach = MAX(contact_source_facts.usable_for_outreach, excluded.usable_for_outreach),
            usable_reason = COALESCE(excluded.usable_reason, contact_source_facts.usable_reason),
            blocked_reason = COALESCE(excluded.blocked_reason, contact_source_facts.blocked_reason),
            notes = COALESCE(excluded.notes, contact_source_facts.notes),
            last_seen_at = CURRENT_TIMESTAMP
        """,
        (
            contact_id,
            company_id,
            source_record_id,
            source_system,
            source_file or None,
            source_sheet or None,
            source_row,
            email_verification_status or None,
            email_domain_type or None,
            float(title_confidence or 0.0),
            float(person_confidence or 0.0),
            float(company_confidence or 0.0),
            float(record_quality_score or 0.0),
            int(usable_for_outreach),
            usable_reason or None,
            blocked_reason or None,
            notes or None,
        ),
    )


def run(db_path, business_line=None, source_filter=None, limit=None):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    _ensure_extensions(conn)
    cur = conn.cursor()

    q = "SELECT * FROM source_records WHERE 1=1"
    params = []
    if business_line:
        q += " AND business_line=?"
        params.append(business_line)
    if source_filter:
        q += " AND source_name=?"
        params.append(source_filter)
    if limit:
        q += f" LIMIT {limit}"

    cur.execute(q, params)
    rows = cur.fetchall()
    print(f"[normalize] Processing {len(rows)} source records...")

    co_added = ct_added = 0
    pipeline_rows = generic_rows = 0

    for row in rows:
        source_record_id = int(row["source_record_id"])
        source_system = (row["source_type"] or "import").strip().lower()

        raw = {}
        try:
            raw = json.loads(row["raw_payload"] or "{}")
        except Exception:
            pass

        source_file = str(raw.get("source_file") or row["source_file"] or row["source_name"] or "")
        source_sheet = str(raw.get("source_sheet") or row["source_sheet"] or "")
        source_row = raw.get("source_row") or row["source_row"]
        try:
            source_row = int(source_row) if source_row is not None else None
        except Exception:
            source_row = None

        if is_pipeline_row(raw):
            pipeline_rows += 1
            orig_co = raw.get("contractor_name_normalized", "").strip()
            domain = (raw.get("contractor_domain", "") or "").strip().lower() or None
            jur = raw.get("jurisdiction", "") or ""
            state = jurisdiction_to_state(jur)
            proj_addr = raw.get("project_address", "") or ""
            if not state and proj_addr:
                m = re.search(r",\s*([A-Z]{2})\s+\d{5}", proj_addr)
                if m:
                    state = m.group(1)
            region = infer_region(state)
            score = raw.get("score", 0) or 0
            base_conf = min(1.0, float(score) / 10.0) if score else 0.5
            mv_status = (raw.get("email_verification_status", "") or "").lower().strip()
            email_verified = 1 if mv_status == "valid" else 0
            email = norm_email(raw.get("email_candidate") or raw.get("email") or row["original_email"] or "")

            company_id = None
            if orig_co or domain:
                norm_co = norm_company(orig_co)
                if domain:
                    cur.execute("SELECT company_id FROM companies WHERE domain=?", (domain,))
                    r2 = cur.fetchone()
                    company_id = r2[0] if r2 else None
                if not company_id and norm_co:
                    cur.execute("SELECT company_id FROM companies WHERE normalized_company_name=?", (norm_co,))
                    r2 = cur.fetchone()
                    company_id = r2[0] if r2 else None
                if company_id and domain:
                    cur.execute("UPDATE companies SET domain=COALESCE(domain, ?), updated_at=CURRENT_TIMESTAMP WHERE company_id=?", (domain, company_id))
                if not company_id:
                    cur.execute(
                        """
                        INSERT INTO companies (company_name, normalized_company_name, domain, location_state, region)
                        VALUES(?,?,?,?,?)
                        """,
                        (orig_co or domain, norm_co, domain, state, region),
                    )
                    company_id = cur.lastrowid
                    co_added += 1

                company_conf = _company_confidence(norm_company(orig_co), domain or "")
                _upsert_company_fact(
                    cur,
                    company_id=company_id,
                    source_record_id=source_record_id,
                    source_system=source_system,
                    source_file=source_file,
                    source_sheet=source_sheet,
                    source_row=source_row,
                    preferred_domain=domain or "",
                    domain_confidence=company_conf,
                    domain_evidence_notes="pipeline_domain" if domain else "pipeline_no_domain",
                    quality_notes="pipeline_normalized",
                )

            if email:
                cur.execute("SELECT contact_id FROM contacts WHERE email=?", (email,))
                existing = cur.fetchone()
                if not existing:
                    local = email.split("@")[0]
                    role_prefixes = {
                        "info", "ops", "estimating", "operations", "project", "pm", "procurement", "dispatch",
                        "equipment", "rental", "fleet", "sales", "admin", "office", "contact", "hello", "jobs", "bid",
                    }
                    if local in role_prefixes:
                        full = ""
                        first = ""
                        last = ""
                        role = local
                        title = local.replace("_", " ").title()
                    else:
                        parts = re.split(r"[.\-_]", local)
                        first = parts[0].capitalize() if parts else ""
                        last = parts[1].capitalize() if len(parts) > 1 else ""
                        full = f"{first} {last}".strip()
                        title = ""
                        role = None
                    cur.execute(
                        """
                        INSERT INTO contacts (
                            company_id, full_name, first_name, last_name, title, contact_role,
                            email, location_state, confidence_score, email_verified
                        ) VALUES(?,?,?,?,?,?,?,?,?,?)
                        """,
                        (company_id, full, first, last, title, role, email, state, base_conf, email_verified),
                    )
                    contact_id = cur.lastrowid
                    ct_added += 1
                else:
                    contact_id = int(existing[0])

                title = (raw.get("title") or "").strip()
                title_conf = _title_confidence(title)
                person_conf = _person_confidence("", "", email, title, base_conf)
                company_conf = _company_confidence(norm_company(orig_co), domain or "")
                domain_type = _email_domain_type(email)
                record_quality = _record_quality_score(person_conf, company_conf, title_conf, domain_type, mv_status)
                usable, usable_reason, blocked_reason = _usability(record_quality, email, domain_type)

                _upsert_contact_fact(
                    cur,
                    contact_id=contact_id,
                    company_id=company_id,
                    source_record_id=source_record_id,
                    source_system=source_system,
                    source_file=source_file,
                    source_sheet=source_sheet,
                    source_row=source_row,
                    email_verification_status=mv_status,
                    email_domain_type=domain_type,
                    title_confidence=title_conf,
                    person_confidence=person_conf,
                    company_confidence=company_conf,
                    record_quality_score=record_quality,
                    usable_for_outreach=usable,
                    usable_reason=usable_reason,
                    blocked_reason=blocked_reason,
                    notes="pipeline_contact",
                )

        else:
            generic_rows += 1

            def get(*keys):
                for k in keys:
                    v = raw.get(k, "")
                    if v and str(v).strip():
                        return str(v).strip()
                return ""

            orig_co = get("company_name", "company", "business_name", "employer", "organization") or row["original_company_name"] or ""
            email = norm_email(get("email", "email_address", "work_email", "contact_email") or row["original_email"] or "")
            domain = get("domain", "website", "company_domain").lower() or None
            if not domain and email and _email_domain_type(email) == "corporate":
                domain = _email_domain(email) or None
            state = get("state", "location_state", "st").upper()[:2] or None
            city = get("city", "location_city") or None
            industry = get("industry", "sector", "vertical") or None
            region = infer_region(state)
            norm_co = norm_company(orig_co)

            company_id = None
            if orig_co or domain:
                if domain:
                    cur.execute("SELECT company_id FROM companies WHERE domain=?", (domain,))
                    r2 = cur.fetchone()
                    company_id = r2[0] if r2 else None
                if not company_id and norm_co:
                    cur.execute("SELECT company_id FROM companies WHERE normalized_company_name=?", (norm_co,))
                    r2 = cur.fetchone()
                    company_id = r2[0] if r2 else None
                if not company_id:
                    cur.execute(
                        """
                        INSERT INTO companies (
                            company_name, normalized_company_name, domain, industry,
                            location_city, location_state, region
                        ) VALUES(?,?,?,?,?,?,?)
                        """,
                        (orig_co, norm_co, domain, industry, city, state, region),
                    )
                    company_id = cur.lastrowid
                    co_added += 1

                company_conf = _company_confidence(norm_co, domain or "")
                _upsert_company_fact(
                    cur,
                    company_id=company_id,
                    source_record_id=source_record_id,
                    source_system=source_system,
                    source_file=source_file,
                    source_sheet=source_sheet,
                    source_row=source_row,
                    preferred_domain=domain or "",
                    domain_confidence=company_conf,
                    domain_evidence_notes="generic_domain" if domain else "generic_no_domain",
                    quality_notes="generic_normalized",
                )

            orig_name = get("full_name", "contact_name", "name", "person_name", "contact") or row["original_person_name"] or ""
            email = norm_email(get("email", "email_address", "work_email", "contact_email") or row["original_email"] or "")
            phone = norm_phone(get("phone", "phone_number", "mobile", "direct_phone") or row["original_phone"] or "")
            linkedin = get("linkedin_url", "linkedin", "profile_url") or None
            title = norm_title(get("title", "job_title", "position", "role"))
            role = infer_role(title)
            verification_status = (get("email_verification_status", "email_verification_result") or "").strip().lower()
            if not verification_status:
                verification_status = "unknown"
            email_verified = 1 if verification_status == "valid" else 0

            if orig_name or email:
                first, last = split_name(orig_name)
                full = f"{first} {last}".strip() or orig_name
                existing = None
                if email:
                    cur.execute("SELECT contact_id FROM contacts WHERE email=?", (email,))
                    r2 = cur.fetchone()
                    existing = r2[0] if r2 else None
                if not existing and linkedin:
                    cur.execute("SELECT contact_id FROM contacts WHERE linkedin_url=?", (linkedin,))
                    r2 = cur.fetchone()
                    existing = r2[0] if r2 else None
                if not existing and full and company_id:
                    cur.execute("SELECT contact_id FROM contacts WHERE full_name=? AND company_id=?", (full, company_id))
                    r2 = cur.fetchone()
                    existing = r2[0] if r2 else None

                if not existing:
                    cur.execute(
                        """
                        INSERT INTO contacts (
                            company_id, full_name, first_name, last_name, title, contact_role,
                            email, phone, linkedin_url, location_city, location_state,
                            confidence_score, email_verified
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            company_id,
                            full,
                            first,
                            last,
                            title,
                            role,
                            email or None,
                            phone or None,
                            linkedin,
                            city,
                            state,
                            0.6,
                            email_verified,
                        ),
                    )
                    contact_id = cur.lastrowid
                    ct_added += 1
                else:
                    contact_id = int(existing)

                title_conf = _title_confidence(title)
                person_conf = _person_confidence(first, last, email, title, 0.5)
                company_conf = _company_confidence(norm_co, domain or "")
                domain_type = _email_domain_type(email)
                record_quality = _record_quality_score(person_conf, company_conf, title_conf, domain_type, verification_status)
                usable, usable_reason, blocked_reason = _usability(record_quality, email, domain_type)

                _upsert_contact_fact(
                    cur,
                    contact_id=contact_id,
                    company_id=company_id,
                    source_record_id=source_record_id,
                    source_system=source_system,
                    source_file=source_file,
                    source_sheet=source_sheet,
                    source_row=source_row,
                    email_verification_status=verification_status,
                    email_domain_type=domain_type,
                    title_confidence=title_conf,
                    person_confidence=person_conf,
                    company_confidence=company_conf,
                    record_quality_score=record_quality,
                    usable_for_outreach=usable,
                    usable_reason=usable_reason,
                    blocked_reason=blocked_reason,
                    notes="generic_contact",
                )

    conn.commit()
    conn.close()
    print(f"[normalize] ✓ Companies added: {co_added} | Contacts added: {ct_added}")
    print(f"[normalize]   Pipeline-format rows: {pipeline_rows} | Generic-format rows: {generic_rows}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--db", default=None)
    p.add_argument("--business_line", default=None)
    p.add_argument("--source", default=None)
    p.add_argument("--limit", default=None, type=int)
    a = p.parse_args()
    db = get_db(a.db)
    if not Path(db).exists():
        sys.exit(f"[normalize] DB not found: {db}")
    run(db, a.business_line, a.source, a.limit)
