#!/usr/bin/env python3
"""
contact_intelligence/scripts/log_feedback.py
Log an outcome event for a contact, company, or domain.
Outcomes feed back into domain scoring and contact pattern confidence.

Outcome types: bounce | reply | wrong_person | verified_domain | false_positive | meeting_set

Usage (from repo root):
    python3 contact_intelligence/scripts/log_feedback.py \
        --email jane@acme.com \
        --outcome bounce \
        --source plusvibe_campaign_jan

    python3 contact_intelligence/scripts/log_feedback.py \
        --domain acme.com \
        --outcome verified_domain \
        --source manual_check

    python3 contact_intelligence/scripts/log_feedback.py \
        --contact_id 42 \
        --outcome meeting_set \
        --detail "Booked call with Lisa - Leavitt Chicago"

    Bulk import from CSV:
    python3 contact_intelligence/scripts/log_feedback.py \
        --file ~/data_runtime/imports/campaign_outcomes.csv
"""

import os, sys, csv, sqlite3, argparse
from pathlib import Path
from datetime import datetime

DEFAULT_DB = os.path.expanduser("~/data_runtime/cranegenius_ci.db")
VALID_OUTCOMES = {"bounce","reply","wrong_person","verified_domain","false_positive","meeting_set","unsubscribed"}


def get_db(arg=None):
    return arg or os.environ.get("CRANEGENIUS_CI_DB", DEFAULT_DB)


def lookup_contact(cur, email):
    if not email: return None, None
    cur.execute("SELECT contact_id, company_id FROM contacts WHERE email=? LIMIT 1", (email.lower(),))
    r = cur.fetchone()
    return (r[0], r[1]) if r else (None, None)


def lookup_company_by_domain(cur, domain):
    if not domain: return None
    cur.execute("SELECT company_id FROM companies WHERE domain=? LIMIT 1", (domain,))
    r = cur.fetchone(); return r[0] if r else None


def log_one(cur, outcome_type, contact_id=None, company_id=None,
            canonical_id=None, email=None, domain=None, detail=None, source=None):
    cur.execute("""INSERT INTO feedback_outcomes
        (company_id,contact_id,canonical_company_id,email_tested,domain_tested,
         outcome_type,outcome_detail,source)
        VALUES(?,?,?,?,?,?,?,?)""",
        (company_id, contact_id, canonical_id,
         email, domain, outcome_type, detail, source))


def run_single(args, conn):
    cur = conn.cursor()
    if args.outcome not in VALID_OUTCOMES:
        sys.exit(f"[feedback] Invalid outcome: {args.outcome}. Use: {VALID_OUTCOMES}")

    contact_id = args.contact_id
    company_id = args.company_id
    canonical_id = None
    email  = args.email
    domain = args.domain

    if email and not contact_id:
        contact_id, company_id = lookup_contact(cur, email)
    if domain and not company_id:
        company_id = lookup_company_by_domain(cur, domain)
        cur.execute("SELECT canonical_company_id FROM canonical_companies WHERE primary_domain=? LIMIT 1", (domain,))
        r = cur.fetchone(); canonical_id = r[0] if r else None

    log_one(cur, args.outcome, contact_id, company_id, canonical_id,
            email, domain, args.detail, args.source)
    conn.commit()
    print(f"[feedback] ✓ Logged: {args.outcome} for email={email} domain={domain} contact_id={contact_id}")


def run_bulk(file_path, source, conn):
    cur = conn.cursor()
    n = 0
    with open(file_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            outcome = row.get("outcome","").strip().lower()
            if not outcome or outcome not in VALID_OUTCOMES:
                print(f"[feedback] Skipping row with invalid outcome: {outcome}")
                continue
            email  = row.get("email","").strip().lower() or None
            domain = row.get("domain","").strip().lower() or None
            detail = row.get("detail","").strip() or None
            contact_id, company_id = lookup_contact(cur, email) if email else (None, None)
            if domain and not company_id:
                company_id = lookup_company_by_domain(cur, domain)
            log_one(cur, outcome, contact_id, company_id, None, email, domain, detail, source)
            n += 1
    conn.commit()
    print(f"[feedback] ✓ Logged {n} outcomes from {file_path}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--email",       default=None)
    p.add_argument("--domain",      default=None)
    p.add_argument("--contact_id",  default=None, type=int)
    p.add_argument("--company_id",  default=None, type=int)
    p.add_argument("--outcome",     default=None)
    p.add_argument("--detail",      default=None)
    p.add_argument("--source",      default="manual")
    p.add_argument("--file",        default=None, help="Bulk import CSV with columns: email,domain,outcome,detail")
    p.add_argument("--db",          default=None)
    a = p.parse_args()
    db = get_db(a.db)
    if not Path(db).exists(): sys.exit(f"[feedback] DB not found: {db}")
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    if a.file:
        run_bulk(a.file, a.source, conn)
    else:
        if not a.outcome:
            p.print_help(); sys.exit(1)
        run_single(a, conn)
    conn.close()
