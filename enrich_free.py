import re, csv, sqlite3
from pathlib import Path
from collections import Counter

DB = Path.home() / "data_runtime" / "cranegenius_ci.db"
conn = sqlite3.connect(DB)
cur = conn.cursor()

FREE_EMAIL = {
    'gmail.com','yahoo.com','hotmail.com','outlook.com','icloud.com','aol.com',
    'shaw.ca','telus.net','rogers.com','hotmail.ca','yahoo.ca','live.com',
    'msn.com','me.com','live.ca','sympatico.ca','mymts.net','sasktel.net',
    'xplornet.com','cogeco.ca','videotron.ca','eastlink.ca','tbaytel.net',
}

# ── 1. EXTRACT DOMAIN FROM BUSINESS EMAIL ────────────────────────
print("=== Fix 1: Extract domains from business emails ===")
domain_updated = 0
contacts = cur.execute("SELECT contact_id, email, company_id FROM contacts WHERE email IS NOT NULL").fetchall()
for cid, email, company_id in contacts:
    if not email or '@' not in email: continue
    domain = email.split('@')[1].strip().lower()
    if domain in FREE_EMAIL: continue
    # Update the company's domain if it doesn't have one
    if company_id:
        existing = cur.execute("SELECT domain FROM companies WHERE company_id=?", (company_id,)).fetchone()
        if existing and not existing[0]:
            cur.execute("UPDATE companies SET domain=? WHERE company_id=?", (domain, company_id))
            domain_updated += 1

conn.commit()
print(f"  Domains filled from email: {domain_updated}")

# ── 2. CITY → STATE/PROVINCE LOOKUP ──────────────────────────────
print("\n=== Fix 2: City → state/province mapping ===")

# Canadian cities → province code
CA = {
    # Alberta
    "calgary":"AB","edmonton":"AB","fort mcmurray":"AB","red deer":"AB",
    "lethbridge":"AB","medicine hat":"AB","grande prairie":"AB","airdrie":"AB",
    "spruce grove":"AB","leduc":"AB","camrose":"AB","lloydminster":"AB",
    "sherwood park":"AB","st. albert":"AB","nisku":"AB","stony plain":"AB",
    "fort saskatchewan":"AB","cold lake":"AB","brooks":"AB","wetaskiwin":"AB",
    "rocky mountain house":"AB","lacombe":"AB","edson":"AB","drayton valley":"AB",
    # BC
    "vancouver":"BC","surrey":"BC","kelowna":"BC","victoria":"BC","burnaby":"BC",
    "richmond":"BC","abbotsford":"BC","coquitlam":"BC","langley":"BC",
    "delta":"BC","nanaimo":"BC","kamloops":"BC","chilliwack":"BC","prince george":"BC",
    "maple ridge":"BC","new westminster":"BC","penticton":"BC","vernon":"BC",
    "port coquitlam":"BC","north vancouver":"BC","west kelowna":"BC","saanich":"BC",
    "aldergrove":"BC","cranbrook":"BC","campbell river":"BC","fort st. john":"BC",
    # Saskatchewan
    "saskatoon":"SK","regina":"SK","prince albert":"SK","moose jaw":"SK",
    "swift current":"SK","yorkton":"SK","north battleford":"SK","estevan":"SK",
    # Manitoba
    "winnipeg":"MB","brandon":"MB","steinbach":"MB","thompson":"MB","portage la prairie":"MB",
    # Ontario
    "toronto":"ON","ottawa":"ON","mississauga":"ON","brampton":"ON","hamilton":"ON",
    "london":"ON","markham":"ON","vaughan":"ON","kitchener":"ON","windsor":"ON",
    "richmond hill":"ON","oakville":"ON","burlington":"ON","greater sudbury":"ON",
    "oshawa":"ON","barrie":"ON","st. catharines":"ON","cambridge":"ON","kingston":"ON",
    "whitby":"ON","guelph":"ON","thunderbay":"ON","thunder bay":"ON","sudbury":"ON",
    # Quebec
    "montreal":"QC","quebec city":"QC","laval":"QC","gatineau":"QC","longueuil":"QC",
    # Other
    "halifax":"NS","moncton":"NB","fredericton":"NB","charlottetown":"PE",
    "st. john's":"NL","whitehorse":"YT","yellowknife":"NT",
}
# US cities → state
US = {
    "seattle":"WA","portland":"OR","spokane":"WA","tacoma":"WA","bellevue":"WA",
    "eugene":"OR","salem":"OR","medford":"OR","bend":"OR","redmond":"OR",
    "boise":"ID","nampa":"ID","meridian":"ID","idaho falls":"ID",
    "phoenix":"AZ","tucson":"AZ","mesa":"AZ","chandler":"AZ","scottsdale":"AZ",
    "denver":"CO","colorado springs":"CO","aurora":"CO","fort collins":"CO",
    "dallas":"TX","houston":"TX","san antonio":"TX","austin":"TX","fort worth":"TX",
    "chicago":"IL","rockford":"IL","peoria":"IL","springfield":"IL",
    "new york":"NY","buffalo":"NY","rochester":"NY","yonkers":"NY",
    "los angeles":"CA","san diego":"CA","san jose":"CA","san francisco":"CA","fresno":"CA",
    "las vegas":"NV","henderson":"NV","reno":"NV",
    "albuquerque":"NM","santa fe":"NM",
    "salt lake city":"UT","provo":"UT","west valley city":"UT",
    "tulsa":"OK","oklahoma city":"OK",
    "houston":"TX","corpus christi":"TX","el paso":"TX",
    "minneapolis":"MN","saint paul":"MN","duluth":"MN",
    "kansas city":"KS","wichita":"KS","topeka":"KS",
    "omaha":"NE","lincoln":"NE",
    "milwaukee":"WI","madison":"WI","green bay":"WI",
    "indianapolis":"IN","fort wayne":"IN","evansville":"IN",
    "columbus":"OH","cleveland":"OH","cincinnati":"OH","toledo":"OH","akron":"OH",
    "detroit":"MI","grand rapids":"MI","warren":"MI","sterling heights":"MI",
    "louisville":"KY","lexington":"KY",
    "nashville":"TN","memphis":"TN","knoxville":"TN","chattanooga":"TN",
    "atlanta":"GA","augusta":"GA","columbus":"GA","savannah":"GA",
    "charlotte":"NC","raleigh":"NC","greensboro":"NC","durham":"NC",
    "virginia beach":"VA","norfolk":"VA","richmond":"VA","arlington":"VA",
    "jacksonville":"FL","miami":"FL","tampa":"FL","orlando":"FL","st. petersburg":"FL",
    "new orleans":"LA","baton rouge":"LA","shreveport":"LA",
    "birmingham":"AL","montgomery":"AL","huntsville":"AL",
    "jackson":"MS","hattiesburg":"MS",
    "little rock":"AR","fort smith":"AR",
    "sulphur springs":"TX",
}

REGION_US = {
    "WA":"West","OR":"West","CA":"West","NV":"West","ID":"West","UT":"West","AZ":"Southwest",
    "CO":"Southwest","TX":"Southwest","NM":"Southwest","OK":"Southwest",
    "IL":"Midwest","OH":"Midwest","MI":"Midwest","IN":"Midwest","WI":"Midwest",
    "MN":"Midwest","IA":"Midwest","MO":"Midwest","KS":"Midwest","NE":"Midwest",
    "NY":"Northeast","PA":"Northeast","NJ":"Northeast","MA":"Northeast","CT":"Northeast",
    "FL":"Southeast","GA":"Southeast","NC":"Southeast","SC":"Southeast","TN":"Southeast",
    "AL":"Southeast","MS":"Southeast","LA":"Southeast","VA":"Southeast","KY":"Southeast",
}
REGION_CA = {
    "AB":"Prairies","BC":"West Coast","SK":"Prairies","MB":"Prairies",
    "ON":"Central","QC":"Central","NS":"Atlantic","NB":"Atlantic",
    "PE":"Atlantic","NL":"Atlantic","YT":"North","NT":"North","NU":"North",
}

state_updated = region_updated = 0
companies_no_state = cur.execute("SELECT company_id FROM companies WHERE location_state IS NULL").fetchall()

# Map contacts' cities to states
for (company_id,) in companies_no_state:
    # Get city from contacts linked to this company
    contact_row = cur.execute("""
        SELECT ct.email FROM contacts ct 
        WHERE ct.company_id=? LIMIT 1
    """, (company_id,)).fetchone()
    if not contact_row: continue
    
    # We need to get city from original import - use company domain as proxy
    # For now mark Canadian companies based on city patterns
    pass

# Better approach: load cities directly from the CSV and update contacts
APOLLO_CSV = None
for candidate in [
    Path.home() / "Downloads" / "5d4729e4-27d9-42ae-9b7a-148d2641873d.csv",
]:
    if candidate.exists():
        APOLLO_CSV = candidate
        break

if APOLLO_CSV:
    with open(APOLLO_CSV, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            email = (row.get("email","") or "").strip().lower()
            city = (row.get("city","") or "").strip().lower()
            if not email or not city: continue
            
            state = US.get(city) or CA.get(city)
            if not state: continue
            
            region = REGION_US.get(state) or REGION_CA.get(state)
            country = "CA" if state in REGION_CA else "US"
            
            # Update contact
            cur.execute("UPDATE contacts SET location_state=? WHERE email=?", (state, email))
            # Update company
            result = cur.execute("SELECT company_id FROM contacts WHERE email=?", (email,)).fetchone()
            if result:
                cur.execute("""
                    UPDATE companies SET location_state=?, region=?, country_code=?
                    WHERE company_id=? AND location_state IS NULL
                """, (state, region, country, result[0]))
                state_updated += 1

conn.commit()
print(f"  State/region filled: {state_updated} contacts")

# ── 3. FLAG LOW PRIORITY CONTACTS ────────────────────────────────
print("\n=== Fix 3: Flag low-priority contacts ===")
# Contacts with personal emails and no title = low priority
flagged = 0
for cid, email in cur.execute("SELECT contact_id, email FROM contacts WHERE title IS NULL OR title=''").fetchall():
    if not email or '@' not in email: continue
    domain = email.split('@')[1].lower()
    if domain in FREE_EMAIL:
        cur.execute("UPDATE contacts SET confidence_score=0.25 WHERE contact_id=?", (cid,))
        flagged += 1

conn.commit()
print(f"  Low-priority flagged (personal email + no title): {flagged}")

# ── REPORT ────────────────────────────────────────────────────────
print("\n=== FINAL DB STATE ===")
print(f"  Total companies: {cur.execute('SELECT COUNT(*) FROM companies').fetchone()[0]}")
print(f"  With domain:     {cur.execute('SELECT COUNT(*) FROM companies WHERE domain IS NOT NULL').fetchone()[0]}")
print(f"  With state:      {cur.execute('SELECT COUNT(*) FROM companies WHERE location_state IS NOT NULL').fetchone()[0]}")
print(f"  Total contacts:  {cur.execute('SELECT COUNT(*) FROM contacts').fetchone()[0]}")
print(f"  With state:      {cur.execute('SELECT COUNT(*) FROM contacts WHERE location_state IS NOT NULL').fetchone()[0]}")
print(f"  Verified email:  {cur.execute('SELECT COUNT(*) FROM contacts WHERE email_verified=1').fetchone()[0]}")
print(f"  High conf(>=0.7):{cur.execute('SELECT COUNT(*) FROM contacts WHERE confidence_score>=0.7').fetchone()[0]}")
print(f"  Low priority:    {cur.execute('SELECT COUNT(*) FROM contacts WHERE confidence_score<=0.25').fetchone()[0]}")

print("\nLocation breakdown (contacts):")
for r in cur.execute("SELECT location_state, COUNT(*) n FROM contacts WHERE location_state IS NOT NULL GROUP BY location_state ORDER BY n DESC LIMIT 12").fetchall():
    print(f"  {r[0]:5}: {r[1]}")

print("\nCountry breakdown (companies):")
for r in cur.execute("SELECT country_code, COUNT(*) n FROM companies WHERE country_code IS NOT NULL GROUP BY country_code ORDER BY n DESC").fetchall():
    print(f"  {r[0]:5}: {r[1]}")

conn.close()
print("\nDone! Run: python3 contact_intelligence/scripts/export_views.py")
