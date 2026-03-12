#!/usr/bin/env python3
"""
contact_intelligence/scripts/run_gold_truth_checks.py
Compare domain_evidence and contacts against gold_truth tables.
Outputs a benchmark accuracy report and optionally promotes verified findings.

Usage (from repo root):
    python3 contact_intelligence/scripts/run_gold_truth_checks.py
    python3 contact_intelligence/scripts/run_gold_truth_checks.py --promote
    python3 contact_intelligence/scripts/run_gold_truth_checks.py --output ~/reports/gold_check.csv
"""

import os, sys, csv, sqlite3, argparse
from pathlib import Path
from datetime import datetime

DEFAULT_DB  = os.path.expanduser("~/data_runtime/cranegenius_ci.db")
DEFAULT_OUT = os.path.expanduser("~/data_runtime/logs")


def get_db(arg=None):
    return arg or os.environ.get("CRANEGENIUS_CI_DB", DEFAULT_DB)


def run(db_path, promote=False, output_dir=None):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur  = conn.cursor()

    cur.execute("SELECT * FROM gold_truth_companies")
    gold_cos = cur.fetchall()
    print(f"[gold] {len(gold_cos)} gold companies to check against...")

    results = []
    domain_tp = domain_fp = domain_miss = 0
    contact_tp = contact_miss = 0

    for gc in gold_cos:
        # Domain check: does our best domain_evidence match gold?
        cur.execute("""
            SELECT de.domain_candidate, de.confidence_score, de.verified_status
            FROM domain_evidence de
            JOIN canonical_companies cc ON cc.canonical_company_id = de.canonical_company_id
            WHERE cc.normalized_name LIKE ?
            ORDER BY de.confidence_score DESC LIMIT 5
        """, (f"%{gc['canonical_name'].lower()[:20]}%",))
        evidence = cur.fetchall()

        verified_domain = gc["verified_domain"] or ""
        best_match = evidence[0]["domain_candidate"] if evidence else None
        domain_hit = best_match and best_match.lower() == verified_domain.lower()

        if domain_hit:
            domain_tp += 1
            status = "domain_correct"
            if promote:
                cur.execute("""UPDATE domain_evidence SET verified_status='verified', last_verified_at=CURRENT_TIMESTAMP
                    WHERE domain_candidate=?""", (verified_domain,))
                cur.execute("""UPDATE canonical_companies SET
                    verified_status='verified', primary_domain=?, updated_at=CURRENT_TIMESTAMP
                    WHERE canonical_company_id=(
                        SELECT canonical_company_id FROM domain_evidence WHERE domain_candidate=? LIMIT 1
                    )""", (verified_domain, verified_domain))
        elif evidence:
            domain_fp += 1
            status = f"domain_wrong(got:{best_match})"
        else:
            domain_miss += 1
            status = "domain_missing"

        # Contact check
        if gc["verified_domain"]:
            cur.execute("""
                SELECT c.full_name, c.email, c.email_verified
                FROM contacts c
                JOIN companies co ON co.company_id = c.company_id
                WHERE co.domain=? LIMIT 20
            """, (gc["verified_domain"],))
            our_contacts = cur.fetchall()
            cur.execute("SELECT * FROM gold_truth_contacts WHERE gold_id=?", (gc["gold_id"],))
            gold_contacts = cur.fetchall()

            for gtc in gold_contacts:
                ge = (gtc["verified_email"] or "").lower()
                match = any(c["email"] and c["email"].lower()==ge for c in our_contacts)
                if match: contact_tp += 1
                else: contact_miss += 1

        results.append({
            "gold_company":  gc["canonical_name"],
            "gold_domain":   gc["verified_domain"],
            "our_best":      best_match,
            "status":        status,
            "evidence_count":len(evidence),
        })

    # Summary
    total_domain = domain_tp + domain_fp + domain_miss
    total_contact = contact_tp + contact_miss
    d_prec = domain_tp / max(domain_tp + domain_fp, 1)
    d_rec  = domain_tp / max(total_domain, 1)
    c_rec  = contact_tp / max(total_contact, 1)

    print(f"\n[gold] === BENCHMARK RESULTS ===")
    print(f"[gold] Domain Precision:  {d_prec:.1%} ({domain_tp}/{domain_tp+domain_fp})")
    print(f"[gold] Domain Recall:     {d_rec:.1%}  ({domain_tp}/{total_domain})")
    print(f"[gold] Contact Recall:    {c_rec:.1%}  ({contact_tp}/{total_contact})")
    if promote:
        print(f"[gold] Verified rows promoted to canonical layer.")

    if promote: conn.commit()

    # Write CSV report
    out_dir = Path(output_dir or DEFAULT_OUT)
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"gold_check_{ts}.csv"
    with open(out_path, "w", newline="") as f:
        w = csv.DictWriter(f, ["gold_company","gold_domain","our_best","status","evidence_count"])
        w.writeheader(); w.writerows(results)
    print(f"[gold] Report: {out_path}")

    conn.close()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--db",      default=None)
    p.add_argument("--promote", action="store_true", help="Promote verified results back into reference layer")
    p.add_argument("--output",  default=None,        help="Output directory for report CSV")
    a = p.parse_args()
    db = get_db(a.db)
    if not Path(db).exists(): sys.exit(f"[gold] DB not found: {db}")
    run(db, a.promote, a.output)
