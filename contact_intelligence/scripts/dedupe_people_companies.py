#!/usr/bin/env python3
"""
contact_intelligence/scripts/dedupe_people_companies.py
Deduplicate companies and contacts using exact + fuzzy matching.
Merges duplicates. Logs uncertain matches for human review.
"""

import os
import sys
import csv
import sqlite3
import argparse
from pathlib import Path
from datetime import datetime

DEFAULT_DB = os.path.expanduser("~/data_runtime/cranegenius_ci.db")
LOG_DIR = os.path.expanduser("~/data_runtime/logs")

try:
    from rapidfuzz import fuzz

    FUZZY = True
except ImportError:
    FUZZY = False

THRESH_AUTO = 92
THRESH_REVIEW = 85


def get_db(arg=None):
    return arg or os.environ.get("CRANEGENIUS_CI_DB", DEFAULT_DB)


def merge_company_source_facts(conn, keep, drop, dry):
    if dry:
        print(f"  [dry] company_source_facts {drop} → {keep}")
        return
    c = conn.cursor()
    c.execute(
        """
        INSERT INTO company_source_facts (
            company_id, source_record_id, source_system, source_file, source_sheet, source_row,
            ingested_at, first_seen_at, last_seen_at,
            source_support_count, preferred_domain, domain_confidence,
            domain_evidence_notes, quality_notes, notes
        )
        SELECT
            ?, source_record_id, source_system, source_file, source_sheet, source_row,
            ingested_at, first_seen_at, last_seen_at,
            source_support_count, preferred_domain, domain_confidence,
            domain_evidence_notes, quality_notes, notes
        FROM company_source_facts
        WHERE company_id=?
        ON CONFLICT(company_id, source_system, preferred_domain) DO UPDATE SET
            source_support_count = company_source_facts.source_support_count + excluded.source_support_count,
            source_record_id = COALESCE(company_source_facts.source_record_id, excluded.source_record_id),
            source_file = COALESCE(company_source_facts.source_file, excluded.source_file),
            source_sheet = COALESCE(company_source_facts.source_sheet, excluded.source_sheet),
            source_row = COALESCE(company_source_facts.source_row, excluded.source_row),
            domain_confidence = MAX(company_source_facts.domain_confidence, excluded.domain_confidence),
            domain_evidence_notes = COALESCE(company_source_facts.domain_evidence_notes, excluded.domain_evidence_notes),
            quality_notes = COALESCE(company_source_facts.quality_notes, excluded.quality_notes),
            notes = COALESCE(company_source_facts.notes, excluded.notes),
            first_seen_at = MIN(company_source_facts.first_seen_at, excluded.first_seen_at),
            last_seen_at = MAX(company_source_facts.last_seen_at, excluded.last_seen_at)
        """,
        (keep, drop),
    )
    c.execute("DELETE FROM company_source_facts WHERE company_id=?", (drop,))


def merge_contact_source_facts(conn, keep, drop, dry):
    if dry:
        print(f"  [dry] contact_source_facts {drop} → {keep}")
        return
    c = conn.cursor()
    c.execute(
        """
        INSERT INTO contact_source_facts (
            contact_id, company_id, source_record_id, source_system, source_file, source_sheet, source_row,
            ingested_at, first_seen_at, last_seen_at,
            email_verification_status, email_domain_type,
            title_confidence, person_confidence, company_confidence, record_quality_score,
            usable_for_outreach, usable_reason, blocked_reason, notes
        )
        SELECT
            ?, company_id, source_record_id, source_system, source_file, source_sheet, source_row,
            ingested_at, first_seen_at, last_seen_at,
            email_verification_status, email_domain_type,
            title_confidence, person_confidence, company_confidence, record_quality_score,
            usable_for_outreach, usable_reason, blocked_reason, notes
        FROM contact_source_facts
        WHERE contact_id=?
        ON CONFLICT(contact_id, source_system, source_file, source_sheet, source_row) DO UPDATE SET
            company_id = COALESCE(contact_source_facts.company_id, excluded.company_id),
            source_record_id = COALESCE(contact_source_facts.source_record_id, excluded.source_record_id),
            email_verification_status = COALESCE(contact_source_facts.email_verification_status, excluded.email_verification_status),
            email_domain_type = COALESCE(contact_source_facts.email_domain_type, excluded.email_domain_type),
            title_confidence = MAX(contact_source_facts.title_confidence, excluded.title_confidence),
            person_confidence = MAX(contact_source_facts.person_confidence, excluded.person_confidence),
            company_confidence = MAX(contact_source_facts.company_confidence, excluded.company_confidence),
            record_quality_score = MAX(contact_source_facts.record_quality_score, excluded.record_quality_score),
            usable_for_outreach = MAX(contact_source_facts.usable_for_outreach, excluded.usable_for_outreach),
            usable_reason = COALESCE(contact_source_facts.usable_reason, excluded.usable_reason),
            blocked_reason = COALESCE(contact_source_facts.blocked_reason, excluded.blocked_reason),
            notes = COALESCE(contact_source_facts.notes, excluded.notes),
            first_seen_at = MIN(contact_source_facts.first_seen_at, excluded.first_seen_at),
            last_seen_at = MAX(contact_source_facts.last_seen_at, excluded.last_seen_at)
        """,
        (keep, drop),
    )
    c.execute("DELETE FROM contact_source_facts WHERE contact_id=?", (drop,))


def merge_companies(conn, keep, drop, dry):
    if dry:
        print(f"  [dry] company {drop} → {keep}")
        return
    c = conn.cursor()

    merge_company_source_facts(conn, keep, drop, dry)

    c.execute(
        """UPDATE companies SET
        domain=COALESCE(domain,(SELECT domain FROM companies WHERE company_id=?)),
        location_city=COALESCE(location_city,(SELECT location_city FROM companies WHERE company_id=?)),
        location_state=COALESCE(location_state,(SELECT location_state FROM companies WHERE company_id=?)),
        industry=COALESCE(industry,(SELECT industry FROM companies WHERE company_id=?)),
        updated_at=CURRENT_TIMESTAMP
        WHERE company_id=?""",
        (drop, drop, drop, drop, keep),
    )

    for t, col in [
        ("contacts", "company_id"),
        ("projects", "company_id"),
        ("signals", "company_id"),
        ("opportunities", "company_id"),
        ("outreach_history", "company_id"),
        ("equipment_fleet", "company_id"),
        ("crane_requirements", "company_id"),
        ("top_target_entries", "company_id"),
        ("domain_evidence", "company_id"),
        ("contact_patterns", "company_id"),
        ("feedback_outcomes", "company_id"),
        ("contact_source_facts", "company_id"),
        ("job_contact_matches", "company_id"),
        ("opportunity_company_matches", "company_id"),
    ]:
        c.execute(f"UPDATE {t} SET {col}=? WHERE {col}=?", (keep, drop))

    c.execute("DELETE FROM companies WHERE company_id=?", (drop,))


def merge_contacts(conn, keep, drop, dry):
    if dry:
        print(f"  [dry] contact {drop} → {keep}")
        return
    c = conn.cursor()

    merge_contact_source_facts(conn, keep, drop, dry)

    c.execute(
        """UPDATE contacts SET
        linkedin_url=COALESCE(linkedin_url,(SELECT linkedin_url FROM contacts WHERE contact_id=?)),
        phone=COALESCE(phone,(SELECT phone FROM contacts WHERE contact_id=?)),
        location_city=COALESCE(location_city,(SELECT location_city FROM contacts WHERE contact_id=?)),
        updated_at=CURRENT_TIMESTAMP
        WHERE contact_id=?""",
        (drop, drop, drop, keep),
    )

    for t, col in [
        ("outreach_history", "contact_id"),
        ("project_contacts", "contact_id"),
        ("opportunities", "contact_id"),
        ("feedback_outcomes", "contact_id"),
        ("gold_truth_contacts", "gold_contact_id"),
        ("job_contact_matches", "contact_id"),
    ]:
        try:
            c.execute(f"UPDATE {t} SET {col}=? WHERE {col}=?", (keep, drop))
        except Exception:
            pass

    c.execute("DELETE FROM contacts WHERE contact_id=?", (drop,))


def dedupe_companies(conn, dry, review):
    c = conn.cursor()
    c.execute("SELECT company_id,company_name,normalized_company_name,domain,location_state FROM companies ORDER BY company_id")
    rows = c.fetchall()
    merged_ids = set()
    n = 0
    for i, a in enumerate(rows):
        if a[0] in merged_ids:
            continue
        for b in rows[i + 1 :]:
            if b[0] in merged_ids:
                continue
            reason = ""
            if a[3] and b[3] and a[3].lower() == b[3].lower():
                reason = "exact_domain"
            elif a[2] and b[2] and a[2] == b[2]:
                reason = "exact_norm_name"
            elif FUZZY and a[1] and b[1]:
                score = fuzz.token_sort_ratio(a[1], b[1])
                if score >= THRESH_AUTO and a[4] and b[4] and a[4] == b[4]:
                    reason = f"fuzzy+state({score})"
                elif score >= THRESH_AUTO and score >= 96:
                    reason = f"fuzzy_high({score})"
                elif score >= THRESH_REVIEW:
                    review.append({"type": "company", "id_a": a[0], "name_a": a[1], "id_b": b[0], "name_b": b[1], "score": score})
            if reason:
                keep, drop = min(a[0], b[0]), max(a[0], b[0])
                merge_companies(conn, keep, drop, dry)
                merged_ids.add(drop)
                n += 1
                print(f"  [co] {drop}→{keep} ({reason})")
    print(f"[dedupe] Companies merged: {n}")


def dedupe_contacts(conn, dry, review):
    c = conn.cursor()
    c.execute("SELECT contact_id,full_name,email,linkedin_url,phone FROM contacts ORDER BY contact_id")
    rows = c.fetchall()
    merged_ids = set()
    n = 0
    for i, a in enumerate(rows):
        if a[0] in merged_ids:
            continue
        for b in rows[i + 1 :]:
            if b[0] in merged_ids:
                continue
            reason = ""
            if a[3] and b[3] and a[3].lower() == b[3].lower():
                reason = "exact_linkedin"
            elif a[2] and b[2] and a[2].lower() == b[2].lower():
                reason = "exact_email"
            elif a[4] and b[4] and a[4] == b[4]:
                reason = "exact_phone"
            elif FUZZY and a[1] and b[1]:
                score = fuzz.token_sort_ratio(a[1], b[1])
                if score >= 96:
                    reason = f"fuzzy_name({score})"
                elif score >= THRESH_REVIEW:
                    review.append({"type": "contact", "id_a": a[0], "name_a": a[1], "id_b": b[0], "name_b": b[1], "score": score})
            if reason:
                keep, drop = min(a[0], b[0]), max(a[0], b[0])
                merge_contacts(conn, keep, drop, dry)
                merged_ids.add(drop)
                n += 1
                print(f"  [ct] {drop}→{keep} ({reason})")
    print(f"[dedupe] Contacts merged: {n}")


def write_review(review):
    if not review:
        return
    Path(LOG_DIR).mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = f"{LOG_DIR}/dedupe_review_{ts}.csv"
    with open(out, "w", newline="") as f:
        w = csv.DictWriter(f, ["type", "id_a", "name_a", "id_b", "name_b", "score"])
        w.writeheader()
        w.writerows(review)
    print(f"[dedupe] Review log: {out}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--db", default=None)
    p.add_argument("--dry-run", action="store_true")
    a = p.parse_args()
    db = get_db(a.db)
    if not Path(db).exists():
        sys.exit(f"[dedupe] DB not found: {db}")
    if not FUZZY:
        print("[dedupe] WARNING: rapidfuzz not installed. Exact matching only. pip install rapidfuzz")
    conn = sqlite3.connect(db)
    review = []
    dedupe_companies(conn, a.dry_run, review)
    dedupe_contacts(conn, a.dry_run, review)
    if not a.dry_run:
        conn.commit()
    conn.close()
    write_review(review)
    print("[dedupe] ✓ Done.")
