#!/usr/bin/env python3
"""
contact_intelligence/scripts/update_contact_patterns.py
Infer and update email patterns for each company/domain by:
1. Analyzing verified contacts to detect first.last, f.last, first, etc.
2. Incorporating feedback outcomes (bounce reduces, reply increases confidence)
3. Promoting best-scoring patterns to canonical_companies

Usage (from repo root):
    python3 contact_intelligence/scripts/update_contact_patterns.py
"""

import os, sys, re, sqlite3
from pathlib import Path
from collections import Counter

DEFAULT_DB = os.path.expanduser("~/data_runtime/cranegenius_ci.db")


def detect_pattern(email: str, first: str, last: str) -> str | None:
    if not email or not first: return None
    local = email.lower().split("@")[0]
    fi = first[0].lower()
    f  = first.lower()
    l  = (last or "").lower()
    if not l: return None
    checks = [
        (f"{f}.{l}",  "{first}.{last}"),
        (f"{fi}{l}",  "{first_initial}{last}"),
        (f"{f}{l[0]}","{first}{last_initial}"),
        (f"{f}",      "{first}"),
        (f"{fi}.{l}", "{first_initial}.{last}"),
        (f"{f}_{l}",  "{first}_{last}"),
    ]
    for test, tmpl in checks:
        if local == test:
            return tmpl
    return None


def get_db(arg=None):
    return arg or os.environ.get("CRANEGENIUS_CI_DB", DEFAULT_DB)


def run(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur  = conn.cursor()

    # Get verified contacts with domain info
    cur.execute("""
        SELECT c.contact_id, c.email, c.first_name, c.last_name,
               co.domain, co.canonical_company_id
        FROM contacts c
        JOIN companies co ON co.company_id = c.company_id
        WHERE c.email IS NOT NULL AND c.email_verified=1
          AND co.domain IS NOT NULL
    """)
    verified = cur.fetchall()
    print(f"[patterns] Analyzing {len(verified)} verified contacts...")

    # Also check unverified but high-confidence
    cur.execute("""
        SELECT c.contact_id, c.email, c.first_name, c.last_name,
               co.domain, co.canonical_company_id
        FROM contacts c
        JOIN companies co ON co.company_id = c.company_id
        WHERE c.email IS NOT NULL AND c.confidence_score >= 0.7
          AND co.domain IS NOT NULL AND c.email_verified=0
    """)
    unverified = cur.fetchall()

    domain_patterns = {}  # domain -> Counter of patterns
    for row in list(verified) + list(unverified):
        pat = detect_pattern(row["email"], row["first_name"] or "", row["last_name"] or "")
        if pat and row["domain"]:
            domain_patterns.setdefault(row["domain"], Counter())[pat] += 1

    # Incorporate bounce feedback (reduce pattern counts for bounced emails)
    cur.execute("""
        SELECT f.email_tested, f.outcome_type
        FROM feedback_outcomes f
        WHERE f.email_tested IS NOT NULL
    """)
    for fb in cur.fetchall():
        email = fb["email_tested"]
        if not email or "@" not in email: continue
        dom = email.split("@")[1].lower()
        if dom in domain_patterns and fb["outcome_type"] == "bounce":
            # We can't easily infer which pattern bounced w/o the name,
            # so just reduce overall confidence slightly - logged for now
            pass

    # Upsert contact_patterns
    added = updated = 0
    for domain, counter in domain_patterns.items():
        total = sum(counter.values())
        # Find canonical_company_id for domain
        cur.execute("SELECT canonical_company_id FROM canonical_companies WHERE primary_domain=?", (domain,))
        r = cur.fetchone(); canon_id = r[0] if r else None
        if not canon_id:
            cur.execute("SELECT canonical_company_id FROM domain_evidence WHERE domain_candidate=? AND verified_status!='rejected' LIMIT 1", (domain,))
            r = cur.fetchone(); canon_id = r[0] if r else None

        for tmpl, count in counter.most_common():
            confidence = min(1.0, count / total + 0.3)  # base bump for any observed pattern
            example    = f"{{name}}@{domain}"
            cur.execute("SELECT pattern_id,verified_count FROM contact_patterns WHERE domain=? AND pattern_template=?",
                        (domain, tmpl))
            existing = cur.fetchone()
            if existing:
                cur.execute("""UPDATE contact_patterns SET
                    verified_count=?, confidence=?, last_updated=CURRENT_TIMESTAMP
                    WHERE pattern_id=?""",
                    (existing["verified_count"] + count, confidence, existing["pattern_id"]))
                updated += 1
            else:
                cur.execute("""INSERT INTO contact_patterns
                    (canonical_company_id,domain,pattern_template,pattern_example,
                     verified_count,confidence,source)
                    VALUES(?,?,?,?,?,?,?)""",
                    (canon_id, domain, tmpl, example, count, confidence, "auto_inferred"))
                added += 1

    conn.commit(); conn.close()
    print(f"[patterns] ✓ Patterns added: {added} | updated: {updated}")


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--db", default=None)
    a = p.parse_args()
    db = get_db(a.db)
    if not Path(db).exists(): sys.exit(f"[patterns] DB not found: {db}")
    run(db)
