import os, re, csv, sqlite3
from pathlib import Path
DB = Path.home() / "data_runtime" / "cranegenius_ci.db"
VER = Path("data/verified_contacts.csv")
CAND = Path("data/candidates.csv")
ENRICH = Path("data/enriched_companies.csv")
if not DB.exists(): exit(f"DB not found: {DB}")
ver = {}
if VER.exists():
    with open(VER, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            e = (row.get("email","") or "").strip().lower()
            if e: ver[e] = (row.get("email_verification_status","") or "").lower()
    print(f"Loaded {len(ver)} verification results")
cands = []
if CAND.exists():
    with open(CAND, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        print(f"candidates.csv headers: {reader.fieldnames}")
        for row in reader: cands.append(dict(row))
    print(f"Loaded {len(cands)} candidates")
else:
    print(f"MISSING: {CAND} -- listing data/")
    for p in Path("data").iterdir(): print(f"  {p.name}")
co_lu = {}
if ENRICH.exists():
    with open(ENRICH, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            d = (row.get("contractor_domain","") or "").strip().lower()
            if d: co_lu[d] = dict(row)
    print(f"Loaded {len(co_lu)} from enriched_companies.csv")
RG = {"TX":"Southwest","OK":"Southwest","NM":"Southwest","AZ":"Southwest","CO":"Southwest","CA":"West","OR":"West","WA":"West","NV":"West","ID":"West","UT":"West","IL":"Midwest","OH":"Midwest","MI":"Midwest","IN":"Midwest","WI":"Midwest","MN":"Midwest","IA":"Midwest","MO":"Midwest","KS":"Midwest","NE":"Midwest","NY":"Northeast","PA":"Northeast","NJ":"Northeast","MA":"Northeast","CT":"Northeast","FL":"Southeast","GA":"Southeast","NC":"Southeast","SC":"Southeast","TN":"Southeast","AL":"Southeast","MS":"Southeast","LA":"Southeast","VA":"Southeast","KY":"Southeast"}
JR = {"chicago":"IL","cook county":"IL","dallas":"TX","houston":"TX","nyc":"NY","new york":"NY","manhattan":"NY","brooklyn":"NY","los angeles":"CA","phoenix":"AZ","denver":"CO","austin":"TX","san antonio":"TX","fort worth":"TX"}
ROLES = {"info","ops","estimating","operations","project","pm","procurement","dispatch","equipment","rental","fleet","sales","admin","office","contact","hello","jobs","bid","estimator","safety","crane","lift","rigging"}
ST = re.compile(r"\b(inc\.?|llc\.?|ltd\.?|corp\.?|company|group|construction|services?|solutions?)\b", re.I)
def nc(n):
    if not n: return ""
    return re.sub(r"\s+"," ", re.sub(r"[^\w\s]","", ST.sub("", n.strip().lower()))).strip()
def j2s(j):
    if not j: return None
    j=j.strip().lower()
    if len(j)==2 and j.upper() in RG: return j.upper()
    for k,v in JR.items():
        if k in j: return v
    return None
def a2s(a):
    if not a: return None
    m=re.search(r",\s*([A-Z]{2})\s+\d{5}",a or "")
    return m.group(1) if m else None
conn=sqlite3.connect(DB); cur=conn.cursor()
for t in ["contacts","companies","source_records"]:
    cur.execute(f"DELETE FROM {t}")
    try: cur.execute(f"DELETE FROM sqlite_sequence WHERE name='{t}'")
    except: pass
conn.commit(); print("\nWiped old data")
ca=cta=sk=0
for cand in cands:
    ek=(cand.get("email_candidate") or cand.get("email","")).strip().lower()
    if not ek: sk+=1; continue
    mv=ver.get(ek,"unknown"); vf=1 if mv=="valid" else 0
    dom=(cand.get("contractor_domain") or cand.get("domain","")).strip().lower() or None
    co=(cand.get("contractor_name_normalized") or cand.get("company_name","")).strip()
    if dom and dom in co_lu and not co:
        co=(co_lu[dom].get("contractor_name_normalized") or co_lu[dom].get("company_name","")).strip()
    jur=cand.get("jurisdiction","") or ""; pa=cand.get("project_address","") or ""
    st=j2s(jur) or a2s(pa); rg=RG.get((st or "").upper())
    sc=0
    try: sc=float(cand.get("score") or cand.get("lift_probability_score") or 0)
    except: pass
    cf=min(1.0,sc/10.0) if sc else 0.5
    cid=None
    if dom:
        r=cur.execute("SELECT company_id FROM companies WHERE domain=?",(dom,)).fetchone()
        cid=r[0] if r else None
    if not cid and co:
        n=nc(co); r=cur.execute("SELECT company_id FROM companies WHERE normalized_company_name=?",(n,)).fetchone()
        cid=r[0] if r else None
    if not cid and (dom or co):
        n=nc(co); cur.execute("INSERT INTO companies(company_name,normalized_company_name,domain,location_state,region) VALUES(?,?,?,?,?)",(co or dom,n,dom,st,rg))
        cid=cur.lastrowid; ca+=1
    if cur.execute("SELECT 1 FROM contacts WHERE email=?",(ek,)).fetchone(): sk+=1; continue
    loc=ek.split("@")[0]
    if loc in ROLES: fu=fi=la=""; ro=loc; ti=loc.title()
    else:
        pts=re.split(r"[.\-_]",loc); fi=pts[0].capitalize() if pts else ""; la=pts[1].capitalize() if len(pts)>1 else ""
        fu=f"{fi} {la}".strip(); ti=""; ro=None
    cur.execute("INSERT INTO contacts(company_id,full_name,first_name,last_name,title,contact_role,email,location_state,confidence_score,email_verified) VALUES(?,?,?,?,?,?,?,?,?,?)",(cid,fu,fi,la,ti,ro,ek,st,cf,vf))
    cta+=1
conn.commit()
print(f"\nCompanies added: {ca}")
print(f"Contacts added:  {cta}")
print(f"Skipped:         {sk}")
print("\nSample:")
for r in cur.execute("SELECT c.company_name,c.domain,c.location_state,ct.email,ct.email_verified FROM companies c JOIN contacts ct ON ct.company_id=c.company_id LIMIT 6").fetchall():
    print(f"  {str(r[0])[:28]:28} | {str(r[1] or '')[:24]:24} | {r[2] or '??'} | {r[3]}")
print("\nBy state:")
for r in cur.execute("SELECT location_state,COUNT(*) n FROM contacts GROUP BY location_state ORDER BY n DESC LIMIT 8").fetchall():
    print(f"  {r[0] or 'None':5}: {r[1]}")
conn.close()
print("\nDone! Next: python3 contact_intelligence/scripts/export_views.py")
