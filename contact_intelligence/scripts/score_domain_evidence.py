#!/usr/bin/env python3
"""
contact_intelligence/scripts/score_domain_evidence.py
Score all domain_evidence rows based on evidence type, verification history,
and feedback outcomes. Updates evidence_score and confidence_score.

Scoring model:
  Base score by evidence_type:
    manual        = 0.95
    email_sig     = 0.90
    permit        = 0.85
    scraped       = 0.75
    linkedin      = 0.75
    website       = 0.70
    directory     = 0.60

  Modifiers:
    +0.10 if any positive feedback outcome (reply, verified_domain)
    -0.15 if any negative feedback outcome (bounce, false_positive)
    +0.05 if multiple evidence types confirm same domain
    +0.10 if verified_status = verified

  Final score clamped to [0.0, 1.0]

Usage (from repo root):
    python3 contact_intelligence/scripts/score_domain_evidence.py
"""

import os, sys, sqlite3
from pathlib import Path

DEFAULT_DB = os.path.expanduser("~/data_runtime/cranegenius_ci.db")

BASE_SCORES = {
    "manual": 0.95, "email_sig": 0.90, "permit": 0.85,
    "scraped": 0.75, "linkedin": 0.75, "website": 0.70,
    "directory": 0.60,
}


def get_db(arg=None):
    return arg or os.environ.get("CRANEGENIUS_CI_DB", DEFAULT_DB)


def run(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur  = conn.cursor()

    cur.execute("SELECT * FROM domain_evidence")
    rows = cur.fetchall()
    updated = 0

    # Pre-fetch feedback counts
    cur.execute("""
        SELECT domain_tested,
               SUM(CASE WHEN outcome_type IN ('reply','verified_domain','meeting_set') THEN 1 ELSE 0 END) AS pos,
               SUM(CASE WHEN outcome_type IN ('bounce','false_positive','wrong_person') THEN 1 ELSE 0 END) AS neg
        FROM feedback_outcomes WHERE domain_tested IS NOT NULL
        GROUP BY domain_tested
    """)
    feedback = {r["domain_tested"]: (r["pos"], r["neg"]) for r in cur.fetchall()}

    # Count evidence type diversity per canonical_company
    cur.execute("""
        SELECT canonical_company_id, domain_candidate, COUNT(DISTINCT evidence_type) as type_count
        FROM domain_evidence GROUP BY canonical_company_id, domain_candidate
    """)
    diversity = {(r["canonical_company_id"], r["domain_candidate"]): r["type_count"]
                 for r in cur.fetchall()}

    for row in rows:
        base = BASE_SCORES.get(row["evidence_type"] or "directory", 0.60)
        score = base

        fb = feedback.get(row["domain_candidate"], (0, 0))
        if fb[0] > 0: score += 0.10
        if fb[1] > 0: score -= 0.15

        div_key = (row["canonical_company_id"], row["domain_candidate"])
        if diversity.get(div_key, 1) > 1: score += 0.05

        if row["verified_status"] == "verified": score += 0.10

        score = max(0.0, min(1.0, score))
        cur.execute("""UPDATE domain_evidence SET
            evidence_score=?, confidence_score=?
            WHERE evidence_id=?""",
            (score, score, row["evidence_id"]))
        updated += 1

    conn.commit(); conn.close()
    print(f"[score_domain] ✓ Scored {updated} domain_evidence rows.")


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--db", default=None)
    a = p.parse_args()
    db = get_db(a.db)
    if not Path(db).exists(): sys.exit(f"[score_domain] DB not found: {db}")
    run(db)
