#!/usr/bin/env python3
"""
contact_intelligence/scripts/import_csv.py
Import a CSV into source_records with preserved row-level provenance.
"""

import os
import sys
import csv
import json
import sqlite3
import argparse
from pathlib import Path

DEFAULT_DB = os.path.expanduser("~/data_runtime/cranegenius_ci.db")
VALID_TYPES = {"permit", "apollo", "manual", "linkedin", "edgar", "seed", "crm", "import"}
VALID_LINES = {"cranegenius", "real_estate", "consulting", "recruiting", "general_bd"}

DEFAULT_MAP = {
    "company_name": ["company_name", "company", "business_name", "employer", "organization"],
    "person_name": ["full_name", "contact_name", "name", "person_name", "contact"],
    "email": ["email", "email_address", "work_email", "contact_email", "primary_email"],
    "phone": ["phone", "phone_number", "mobile", "direct_phone", "office_phone"],
    "domain": ["domain", "website", "company_domain", "web"],
    "title": ["title", "job_title", "position", "role"],
    "industry": ["industry", "sector", "vertical"],
    "city": ["city", "location_city"],
    "state": ["state", "location_state", "st"],
    "linkedin_url": ["linkedin_url", "linkedin", "profile_url"],
    "notes": ["notes", "comment", "comments"],
}


def get_db(arg=None):
    return arg or os.environ.get("CRANEGENIUS_CI_DB", DEFAULT_DB)


def resolve(headers: list, mapping: dict) -> dict:
    hl = [h.lower().strip() for h in headers]
    out = {}
    for canon, candidates in mapping.items():
        for c in candidates:
            if c.lower() in hl:
                out[canon] = headers[hl.index(c.lower())]
                break
    return out


def _ensure_source_record_provenance_columns(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(source_records)")
    existing = {row[1] for row in cur.fetchall()}
    alters = []
    if "source_file" not in existing:
        alters.append("ALTER TABLE source_records ADD COLUMN source_file TEXT")
    if "source_sheet" not in existing:
        alters.append("ALTER TABLE source_records ADD COLUMN source_sheet TEXT")
    if "source_row" not in existing:
        alters.append("ALTER TABLE source_records ADD COLUMN source_row INTEGER")
    for sql in alters:
        cur.execute(sql)
    conn.commit()


def import_csv(file_path, source_name, source_type, business_line, mappings_path=None, db_path=None):
    if source_type not in VALID_TYPES:
        sys.exit(f"[import] invalid source_type: {source_type}. Use: {VALID_TYPES}")
    if business_line not in VALID_LINES:
        sys.exit(f"[import] invalid business_line: {business_line}. Use: {VALID_LINES}")

    db_path = get_db(db_path)
    if not Path(db_path).exists():
        sys.exit(f"[import] DB not found: {db_path}. Run init_db.py first.")

    col_map = DEFAULT_MAP.copy()
    if mappings_path and Path(mappings_path).exists():
        custom = json.loads(Path(mappings_path).read_text())
        col_map.update({k: v for k, v in custom.items() if not k.startswith("_")})

    conn = sqlite3.connect(db_path)
    _ensure_source_record_provenance_columns(conn)
    cur = conn.cursor()

    imported = 0
    source_file_name = Path(file_path).name

    with open(file_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        cm = resolve(reader.fieldnames or [], col_map)
        for idx, row in enumerate(reader, start=2):
            payload_dict = dict(row)
            payload_dict.setdefault("source_file", source_file_name)
            payload_dict.setdefault("source_sheet", "csv")
            payload_dict.setdefault("source_row", idx)
            payload = json.dumps(payload_dict, ensure_ascii=False)

            cur.execute(
                """
                INSERT INTO source_records
                    (source_name, source_type, business_line,
                     original_company_name, original_person_name,
                     original_email, original_phone, raw_payload,
                     source_file, source_sheet, source_row)
                VALUES(?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    source_name,
                    source_type,
                    business_line,
                    row.get(cm.get("company_name", ""), "").strip() or None,
                    row.get(cm.get("person_name", ""), "").strip() or None,
                    row.get(cm.get("email", ""), "").strip() or None,
                    row.get(cm.get("phone", ""), "").strip() or None,
                    payload,
                    source_file_name,
                    "csv",
                    idx,
                ),
            )
            imported += 1

    conn.commit()
    conn.close()
    print(f"[import] ✓ {imported} records from {file_path}")
    print(f"[import]   source={source_name} type={source_type} line={business_line}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--file", required=True)
    p.add_argument("--source", required=True)
    p.add_argument("--type", required=True)
    p.add_argument("--business_line", required=True)
    p.add_argument("--mappings", default=None)
    p.add_argument("--db", default=None)
    a = p.parse_args()
    import_csv(a.file, a.source, a.type, a.business_line, a.mappings, a.db)
