import re, csv, sqlite3
from pathlib import Path
from collections import Counter

DB = Path.home() / "data_runtime" / "cranegenius_ci.db"

# Point this at wherever you saved the uploaded file
# Try common download locations
APOLLO_CSV = None
for candidate in [
    Path.home() / "Downloads" / "5d4729e4-27d9-42ae-9b7a-148d2641873d.csv",
    Path.home() / "Downloads" / "apollo_export.csv",
    Path.home() / "Desktop" / "apollo_export.csv",
]:
    if candidate.exists():
        APOLLO_CSV = candidate
        break

if not APOLLO_CSV:
    print("CSV not found in Downloads or Desktop.")
    print("Save the uploaded file to ~/Downloads/ and re-run.")
    exit(1)

print(f"Loading from: {APOLLO_CSV}")

REGION = {
    "TX":"Southwest","OK":"Southwest","NM":"Southwest","AZ":"Southwest","CO":"Southwest",
    "CA":"West","OR":"West","WA":"West","NV":"West","ID":"West","UT":"West",
    "IL":"Midwest","OH":"Midwest","MI":"Midwest","IN":"Midwest","WI":"Midwest",
    "MN":"Midwest","IA":"Midwest","MO":"Midwest","KS":"Midwest","NE":"Midwest",
    "NY":"Northeast","PA":"Northeast","NJ":"Northeast","MA":"Northeast","CT":"Northeast",
    "FL":"Southeast","GA":"Southeast","NC":"Southeast","SC":"Southeast","TN":"Southeast",
    "AL":"Southeast","MS":"Southeast","LA":"Southeast","VA":"Southeast","KY":"Southeast",
}
STRIP = re.compile(r"\b(inc\.?|llc\.?|ltd\.?|corp\.?|company|group|construction|services?|solutions?)\b", re.I)

def nc(n):
    if not n: return ""
    return re.sub(r"\s+"," ", re.sub(r"[^\w\s]","", STRIP.sub("", n.strip().lower()))).strip()

def title_to_conf(title):
    t = (title or "").lower()
    if any(w in t for w in ["owner","president","ceo","principal","founder"]): return 0.88
    if any(w in t for w in ["vp","vice president","director"]): return 0.82
    if any(w in t for w in ["general manager","gm"]): return 0.78
    if any(w in t for w in ["project manager","pm","superintendent"]): return 0.72
    if any(w in t for w in ["estimator","purchaser","procurement","operations manager"]): return 0.70
    if any(w in t for w in ["manager","operations"]): return 0.65
    return 0.55

def domain_from_website(w):
    if not w: return None
    w = w.strip().lower().replace("https://","").replace("http://","").replace("www.","")
    return w.split("/")[0] or None

conn = sqlite3.connect(DB)
cur = conn.cursor()

rows = list(csv.DictReader(open(APOLLO_CSV, encoding="utf-8-sig")))
print(f"Loaded {len(rows)} rows from Apollo CSV")

co_added = ct_added = sk_dup = sk_noemail = 0

for row in rows:
    email = (row.get("email","") or "").strip().lower()
    if not email: sk_noemail += 1; continue

    mv = (row.get("email_verification_result","") or "").lower()
    verified = 1 if mv == "valid" else 0

    company_name = (row.get("company_name","") or "").strip()
    domain = domain_from_website(row.get("company_website",""))
    if not domain and email and "@" in email:
        d = email.split("@")[1]
        # Skip free email providers
        if not any(x in d for x in ["gmail","yahoo","hotmail","outlook","icloud","aol"]):
            domain = d

    state = (row.get("state","") or "").strip().upper() or None
    if state and len(state) > 2:
        state = None  # skip full state names for now
    region = REGION.get(state or "")

    conf = title_to_conf(row.get("job_title",""))

    # Upsert company
    company_id = None
    if domain:
        r2 = cur.execute("SELECT company_id FROM companies WHERE domain=?", (domain,)).fetchone()
        company_id = r2[0] if r2 else None
    if not company_id and company_name:
        ncn = nc(company_name)
        r2 = cur.execute("SELECT company_id FROM companies WHERE normalized_company_name=?", (ncn,)).fetchone()
        company_id = r2[0] if r2 else None
    if not company_id and (domain or company_name):
        ncn = nc(company_name)
        cur.execute(
            "INSERT INTO companies(company_name,normalized_company_name,domain,location_state,region) VALUES(?,?,?,?,?)",
            (company_name or domain, ncn, domain, state, region)
        )
        company_id = cur.lastrowid
        co_added += 1

    # Skip duplicate emails
    if cur.execute("SELECT 1 FROM contacts WHERE email=?", (email,)).fetchone():
        sk_dup += 1; continue

    first = (row.get("first_name","") or "").strip().title()
    last = (row.get("last_name","") or "").strip().title()
    full = f"{first} {last}".strip()
    title = (row.get("job_title","") or "").strip()
    phone = (row.get("phone_number","") or "").strip() or None
    linkedin = (row.get("linkedin_person_url","") or "").strip() or None

    cur.execute(
        """INSERT INTO contacts
        (company_id,full_name,first_name,last_name,title,email,
         location_state,confidence_score,email_verified,phone,linkedin_url)
        VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
        (company_id, full, first, last, title, email,
         state, conf, verified, phone, linkedin)
    )
    ct_added += 1

conn.commit()

print(f"\nCompanies added: {co_added}")
print(f"Contacts added:  {ct_added}")
print(f"Skipped (dup):   {sk_dup}")
print(f"Skipped (no email): {sk_noemail}")

print("\nConfidence distribution:")
for r in cur.execute("SELECT ROUND(confidence_score,2) b, COUNT(*) n FROM contacts GROUP BY b ORDER BY b DESC").fetchall():
    bar = "#" * (r[1] // 20)
    print(f"  {r[0]:.2f}: {r[1]:4}  {bar}")

print("\nTop companies:")
for r in cur.execute("""
    SELECT c.company_name, COUNT(ct.contact_id) n, c.location_state
    FROM companies c JOIN contacts ct ON ct.company_id=c.company_id
    GROUP BY c.company_id ORDER BY n DESC LIMIT 10
""").fetchall():
    print(f"  {str(r[0])[:40]:40} | {r[2] or '??':4} | {r[1]} contacts")

print("\nTotal in DB:")
print(f"  Companies: {cur.execute('SELECT COUNT(*) FROM companies').fetchone()[0]}")
print(f"  Contacts:  {cur.execute('SELECT COUNT(*) FROM contacts').fetchone()[0]}")
print(f"  Verified:  {cur.execute('SELECT COUNT(*) FROM contacts WHERE email_verified=1').fetchone()[0]}")

conn.close()
print("\nDone! Run: python3 contact_intelligence/scripts/export_views.py")
