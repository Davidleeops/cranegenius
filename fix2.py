import re, csv, sqlite3
from pathlib import Path
from collections import Counter

DB = Path.home() / "data_runtime" / "cranegenius_ci.db"
CAND = Path("data/candidates.csv")
conn = sqlite3.connect(DB)
cur = conn.cursor()

# ── DIAGNOSTIC: what are actual role_bucket values? ──
if CAND.exists():
    with open(CAND, newline="", encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    buckets = Counter(r.get("contact_role_bucket","").strip().lower() for r in rows)
    print("Actual role_bucket values:")
    for v,c in buckets.most_common(15):
        print(f"  {repr(v):30} : {c}")
    print()

# ── FIX CONFIDENCE using actual bucket values ──
ROLE_CONF = {
    "owner":           0.85,
    "decision maker":  0.82,
    "decision_maker":  0.82,
    "estimator":       0.75,
    "estimating":      0.75,
    "project manager": 0.72,
    "project_manager": 0.72,
    "pm":              0.70,
    "procurement":     0.70,
    "operations":      0.65,
    "operations manager": 0.65,
    "equipment":       0.62,
    "fleet":           0.62,
    "superintendent":  0.68,
    "foreman":         0.60,
    "sales":           0.55,
    "admin":           0.45,
    "info":            0.40,
    "generic":         0.45,
    "":                0.40,
}

if CAND.exists():
    updated = 0
    for row in rows:
        ek = (row.get("email_candidate") or "").strip().lower()
        rb = (row.get("contact_role_bucket") or "").strip().lower()
        sc = 0
        try: sc = float(row.get("score") or 0)
        except: pass
        if not ek: continue
        conf = ROLE_CONF.get(rb, 0.45)
        if sc > 0: conf = max(conf, min(1.0, sc/10.0))
        cur.execute("UPDATE contacts SET confidence_score=? WHERE email=?", (conf, ek))
        updated += 1
    conn.commit()
    print(f"Updated {updated} confidence scores")

# ── FIX COMPANY NAMES: strip trailing punctuation/numbers ──
junk = re.compile(r'[,.\s]+\d*[,.\s]*$')
co_fixed = 0
for cid, name in cur.execute("SELECT company_id, company_name FROM companies").fetchall():
    clean = junk.sub("", (name or "")).strip()
    clean = re.sub(r'\s{2,}', ' ', clean).strip()
    if clean != name:
        cur.execute("UPDATE companies SET company_name=? WHERE company_id=?", (clean, cid))
        co_fixed += 1
conn.commit()
print(f"Cleaned {co_fixed} company names")

# ── VERIFY ──
print("\nConfidence distribution:")
for r in cur.execute("SELECT ROUND(confidence_score,1) b, COUNT(*) n FROM contacts GROUP BY b ORDER BY b DESC").fetchall():
    bar = "#" * (r[1] // 50)
    print(f"  {r[0]:.1f}: {r[1]:4}  {bar}")

print("\nTop companies by contact count:")
for r in cur.execute("""
    SELECT c.company_name, c.domain, c.location_state, COUNT(ct.contact_id) n
    FROM companies c JOIN contacts ct ON ct.company_id=c.company_id
    GROUP BY c.company_id ORDER BY n DESC LIMIT 8
""").fetchall():
    print(f"  {str(r[0])[:32]:32} | {str(r[1] or '')[:22]:22} | {r[2] or '??':4} | {r[3]} contacts")

print("\nHigh confidence contacts (>=0.75):")
hc = cur.execute("SELECT COUNT(*) FROM contacts WHERE confidence_score>=0.75").fetchone()[0]
print(f"  {hc} contacts with score >= 0.75")

conn.close()
print("\nDone! Now run: python3 contact_intelligence/scripts/export_views.py")
