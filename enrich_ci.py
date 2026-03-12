import re, csv, sqlite3
from pathlib import Path
from collections import defaultdict

DB = Path.home() / "data_runtime" / "cranegenius_ci.db"
CAND = Path("data/candidates.csv")

conn = sqlite3.connect(DB)
cur = conn.cursor()

# ── 1. FIX CONFIDENCE SCORES based on role bucket ──────────────
# role_bucket -> confidence boost
ROLE_CONF = {
    "owner": 0.85,
    "decision_maker": 0.80,
    "estimator": 0.75,
    "project_manager": 0.70,
    "procurement": 0.70,
    "operations": 0.65,
    "equipment": 0.65,
    "generic": 0.45,
}

# Load role buckets from candidates.csv
role_map = {}
if CAND.exists():
    with open(CAND, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            ek = (row.get("email_candidate") or "").strip().lower()
            rb = (row.get("contact_role_bucket") or "generic").strip().lower()
            sc = 0
            try: sc = float(row.get("score") or 0)
            except: pass
            if ek:
                role_map[ek] = (rb, sc)

updated = 0
for email, (rb, sc) in role_map.items():
    if sc > 0:
        conf = min(1.0, sc / 10.0)
    else:
        conf = ROLE_CONF.get(rb, ROLE_CONF.get("generic", 0.45))
    cur.execute("UPDATE contacts SET confidence_score=? WHERE email=?", (conf, email))
    updated += 1

conn.commit()
print(f"Updated confidence scores: {updated} contacts")

# ── 2. ASSIGN SECTOR_IDs based on domain/company name patterns ──
sectors = {r[0]: r[1] for r in cur.execute("SELECT sector_id, sector_name FROM sectors").fetchall()}
print(f"Loaded {len(sectors)} sectors: {list(sectors.values())[:5]}...")

# keyword -> sector_name mapping
SECTOR_KW = {
    "Data Center Construction":       ["data center","datacenter","colocation","colo","server","cloud","hyperscale"],
    "Utility Grid Infrastructure":    ["utility","power","electric","grid","substation","transmission","pge","dte","comed"],
    "Wind Energy Projects":           ["wind","turbine","renewable","solar farm","photovoltaic"],
    "Solar Farm Construction":        ["solar","photovoltaic","pv farm"],
    "LNG and Natural Gas Infrastructure": ["lng","natural gas","pipeline","gas plant","compressor station"],
    "Industrial Shutdown and Turnaround": ["refinery","turnaround","shutdown","maintenance","plant services","industrial services"],
    "Petrochemical Facilities":       ["petro","chemical","refinery","plastics","ethylene"],
    "Steel and Heavy Manufacturing":  ["steel","metal","fabrication","manufacturing","foundry"],
    "Water Treatment Infrastructure": ["water","wastewater","treatment","municipal water","sewer"],
    "Bridge and Civil Infrastructure":["bridge","civil","highway","infrastructure","dot","department of transportation"],
    "Airport Construction and Expansion": ["airport","aviation","terminal","runway","faa"],
    "Port and Maritime Infrastructure":["port","marine","maritime","dock","harbor","shipping"],
    "Modular and Prefabricated Construction": ["modular","prefab","offsite","manufactured"],
    "Mining Infrastructure":          ["mining","mine","quarry","aggregate","mineral"],
    "Battery and EV Manufacturing":   ["battery","ev","electric vehicle","lithium","gigafactory"],
    "Nuclear Plant Refurbishment":    ["nuclear","nrc","reactor","decommission"],
    "Semiconductor Manufacturing":    ["semiconductor","chip","fab","wafer","intel","tsmc","samsung"],
    "LNG Storage Tank Construction":  ["lng tank","cryogenic","storage tank","lpg"],
    "Disaster Recovery Infrastructure":["disaster","fema","recovery","emergency","storm"],
    "Highway Infrastructure":         ["highway","road","paving","asphalt","dot","freeway"],
}

# Invert to lookup
kw_to_sector = {}
for sname, kws in SECTOR_KW.items():
    for kw in kws:
        kw_to_sector[kw.lower()] = sname

def guess_sector(company_name, domain):
    text = ((company_name or "") + " " + (domain or "")).lower()
    for kw, sname in kw_to_sector.items():
        if kw in text:
            return sname
    # Default construction companies → Highway Infrastructure (most generic)
    if any(w in text for w in ["construction","contracting","contractor","builders","building"]):
        return "Bridge and Civil Infrastructure"
    return None

# Build sector_name -> sector_id
name_to_sid = {v: k for k, v in sectors.items()}

assigned = 0
for co in cur.execute("SELECT company_id, company_name, domain FROM companies WHERE sector_id IS NULL").fetchall():
    cid, cname, dom = co
    sname = guess_sector(cname, dom)
    if sname and sname in name_to_sid:
        cur.execute("UPDATE companies SET sector_id=? WHERE company_id=?", (name_to_sid[sname], cid))
        assigned += 1

conn.commit()
print(f"Assigned sector_id to {assigned} companies")

# ── 3. REPORT ────────────────────────────────────────────────────
print("\nConfidence score distribution:")
for r in cur.execute("SELECT ROUND(confidence_score,1) b, COUNT(*) n FROM contacts GROUP BY b ORDER BY b DESC").fetchall():
    print(f"  {r[0]:.1f}: {r[1]}")

print("\nSector assignment:")
for r in cur.execute("""
    SELECT s.sector_name, COUNT(DISTINCT c.company_id) companies, COUNT(ct.contact_id) contacts
    FROM sectors s
    LEFT JOIN companies c ON c.sector_id=s.sector_id
    LEFT JOIN contacts ct ON ct.company_id=c.company_id
    GROUP BY s.sector_id ORDER BY contacts DESC LIMIT 10
""").fetchall():
    print(f"  {str(r[0])[:40]:40} | co={r[1]:3} | ct={r[2]:4}")

print("\nVerified contacts (email_verified=1):")
for r in cur.execute("""
    SELECT ct.email, c.company_name, ct.confidence_score
    FROM contacts ct JOIN companies c ON c.company_id=ct.company_id
    WHERE ct.email_verified=1 LIMIT 10
""").fetchall():
    print(f"  {r[0]:45} | {str(r[1])[:28]:28} | conf={r[2]:.2f}")

conn.close()
print("\nDone! Now re-run: python3 contact_intelligence/scripts/export_views.py")
