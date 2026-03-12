#!/usr/bin/env python3
"""
contact_intelligence/scripts/search_cli.py
Terminal search across CraneGenius contact intelligence database.

Usage (from repo root):
    python3 contact_intelligence/scripts/search_cli.py --state TX --verified
    python3 contact_intelligence/scripts/search_cli.py --sector "Data Center" --state TX
    python3 contact_intelligence/scripts/search_cli.py --company "tower crane" --role operations
    python3 contact_intelligence/scripts/search_cli.py --signal industrial_turnaround --region Southwest
    python3 contact_intelligence/scripts/search_cli.py --domain "leavittcranes.com"
    python3 contact_intelligence/scripts/search_cli.py --crane_type crawler --state LA
    python3 contact_intelligence/scripts/search_cli.py --tier 1 --format csv > tier1.csv
    python3 contact_intelligence/scripts/search_cli.py --title "project manager" --state IL --verified
"""

import os, sys, csv, sqlite3, argparse
from pathlib import Path

DEFAULT_DB = os.path.expanduser("~/data_runtime/cranegenius_ci.db")

BASE = """
SELECT DISTINCT
    s.sector_name       AS sector,
    co.company_name     AS company,
    co.domain,
    co.company_type     AS co_type,
    co.location_city    AS city,
    co.location_state   AS state,
    co.region,
    co.target_tier      AS tier,
    c.full_name         AS contact_name,
    c.title,
    c.contact_role      AS role,
    c.email,
    CASE c.email_verified WHEN 1 THEN 'yes' ELSE 'no' END AS verified,
    c.phone,
    c.linkedin_url      AS linkedin,
    c.confidence_score  AS confidence
FROM contacts c
JOIN companies co ON co.company_id = c.company_id
LEFT JOIN sectors s ON s.sector_id = co.sector_id
LEFT JOIN signals sig ON sig.company_id = co.company_id
LEFT JOIN crane_requirements cr ON cr.company_id = co.company_id
WHERE 1=1
"""


def build(args):
    q, p = BASE, []
    if args.company:       q += " AND co.company_name LIKE ?"; p.append(f"%{args.company}%")
    if args.domain:        q += " AND co.domain LIKE ?";        p.append(f"%{args.domain}%")
    if args.sector:        q += " AND s.sector_name LIKE ?";    p.append(f"%{args.sector}%")
    if args.name:          q += " AND c.full_name LIKE ?";       p.append(f"%{args.name}%")
    if args.title:         q += " AND c.title LIKE ?";           p.append(f"%{args.title}%")
    if args.role:          q += " AND c.contact_role LIKE ?";    p.append(f"%{args.role}%")
    if args.email:         q += " AND c.email LIKE ?";           p.append(f"%{args.email}%")
    if args.industry:      q += " AND co.industry LIKE ?";       p.append(f"%{args.industry}%")
    if args.city:          q += " AND co.location_city LIKE ?";  p.append(f"%{args.city}%")
    if args.state:         q += " AND co.location_state=?";      p.append(args.state.upper())
    if args.region:        q += " AND co.region LIKE ?";         p.append(f"%{args.region}%")
    if args.tier:          q += " AND co.target_tier=?";         p.append(args.tier)
    if args.business_line: q += " AND oh.business_line=?";       p.append(args.business_line)
    if args.signal:        q += " AND sig.signal_type LIKE ?";   p.append(f"%{args.signal}%")
    if args.crane_type:    q += " AND cr.crane_type_needed LIKE ?"; p.append(f"%{args.crane_type}%")
    if args.verified:      q += " AND c.email_verified=1"
    if args.has_phone:     q += " AND c.phone IS NOT NULL"
    if args.has_linkedin:  q += " AND c.linkedin_url IS NOT NULL"
    q += f" ORDER BY c.confidence_score DESC, c.email_verified DESC LIMIT {args.limit}"
    return q, p


def fmt_table(rows, headers):
    if not rows: print("No results."); return
    widths = [min(max(len(h), max(len(str(r[i] or "")) for r in rows)), 38)
              for i, h in enumerate(headers)]
    def fmt(r): return " | ".join(str(v or "")[:widths[i]].ljust(widths[i]) for i, v in enumerate(r))
    sep = "-+-".join("-"*w for w in widths)
    print(fmt(headers)); print(sep)
    for r in rows: print(fmt(r))
    print(f"\n{len(rows)} result(s)")


def fmt_csv(rows, headers):
    w = csv.writer(sys.stdout)
    w.writerow(headers)
    for r in rows: w.writerow([v or "" for v in r])


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Search CraneGenius contact intelligence DB")
    p.add_argument("--company");      p.add_argument("--domain")
    p.add_argument("--sector");       p.add_argument("--name")
    p.add_argument("--title");        p.add_argument("--role")
    p.add_argument("--email");        p.add_argument("--industry")
    p.add_argument("--city");         p.add_argument("--state")
    p.add_argument("--region");       p.add_argument("--business_line")
    p.add_argument("--signal");       p.add_argument("--crane_type")
    p.add_argument("--tier",          type=int)
    p.add_argument("--verified",      action="store_true")
    p.add_argument("--has_phone",     action="store_true")
    p.add_argument("--has_linkedin",  action="store_true")
    p.add_argument("--limit",         type=int, default=50)
    p.add_argument("--format",        choices=["table","csv"], default="table")
    p.add_argument("--db",            default=None)
    a = p.parse_args()

    filters = [a.company,a.domain,a.sector,a.name,a.title,a.role,a.email,a.industry,
               a.city,a.state,a.region,a.business_line,a.signal,a.crane_type,a.tier,
               a.verified,a.has_phone,a.has_linkedin]
    if not any(filters): p.print_help(); sys.exit(0)

    db = a.db or os.environ.get("CRANEGENIUS_CI_DB", DEFAULT_DB)
    if not Path(db).exists(): sys.exit(f"DB not found: {db}. Run init_db.py first.")

    conn = sqlite3.connect(db)
    cur  = conn.cursor()
    q, params = build(a)
    cur.execute(q, params)
    rows    = cur.fetchall()
    headers = [d[0] for d in cur.description]
    conn.close()

    if a.format == "csv": fmt_csv(rows, headers)
    else:                  fmt_table(rows, headers)
