"""
CraneGenius Intel #5: BLM Mining Permits + Solar ROW Applications
Two completely uncontested crane markets:
1. Nevada mining — expansions, processing facilities, heap leach pads
2. Utility-scale solar — transformer/substation crane work

BLM LR2000: https://www.blm.gov/lr2000
Solar ROW: https://eplanning.blm.gov/eplanning-ui/project/

Run: python 05_blm_mining_solar_scraper.py
"""

import requests
from bs4 import BeautifulSoup
import csv
import time
import json
from datetime import datetime, timedelta

OUTPUT_FILE = "blm_mining_solar_leads.csv"
HEADERS = {
    "User-Agent": "Mozilla/5.0 CraneGenius Research",
    "Accept": "application/json, text/html",
}

# Nevada counties with active mining
NV_MINING_COUNTIES = [
    "Elko", "Lander", "Eureka", "Humboldt", "Mineral", 
    "Nye", "White Pine", "Churchill", "Lyon", "Storey",
    "Washoe", "Clark", "Pershing",
]

# Texas/Nevada counties for solar
SOLAR_TARGET_STATES = ["NV", "TX", "CA", "AZ"]

# EPC firms that build utility-scale solar (these are your actual customers)
SOLAR_EPC_FIRMS = [
    {"name": "Blattner Energy", "phone": "320-251-9490", "hq": "Avon, MN"},
    {"name": "Primoris Services", "phone": "214-740-5600", "hq": "Dallas, TX"},
    {"name": "Rosendin Electric", "phone": "408-286-2800", "hq": "San Jose, CA"},
    {"name": "McCarthy Building Companies", "phone": "314-968-3300", "hq": "St. Louis, MO"},
    {"name": "Mortenson Construction", "phone": "763-522-2100", "hq": "Minneapolis, MN"},
    {"name": "Sundt Construction", "phone": "602-293-3000", "hq": "Phoenix, AZ"},
    {"name": "Ames Construction", "phone": "952-435-7106", "hq": "Burnsville, MN"},
    {"name": "Swinerton Renewable Energy", "phone": "303-293-3000", "hq": "Denver, CO"},
    {"name": "IEA Energy Services", "phone": "317-592-2190", "hq": "Indianapolis, IN"},
    {"name": "Capital Power", "phone": "780-392-5500", "hq": "Edmonton, AB"},
]

# Known Nevada mining companies actively expanding
NV_MINING_COMPANIES = [
    {
        "name": "Nevada Gold Mines (Barrick/Newmont JV)",
        "area": "Elko/Carlin Trend",
        "county": "Elko County",
        "crane_use": "Mill equipment, conveyor replacements, underground development hoists",
        "contact_strategy": "Mine maintenance superintendent, not surface ops manager",
        "blm_url": "https://www.blm.gov/nevada/mining",
    },
    {
        "name": "Lithium Nevada (Thacker Pass)",
        "area": "Humboldt County",
        "county": "Humboldt County",
        "crane_use": "Processing plant construction — massive crane requirement, EPC under contract now",
        "contact_strategy": "Contact GC directly — this project is actively under construction 2025-2026",
        "blm_url": "https://eplanning.blm.gov/eplanning-ui/project/2021010/510",
    },
    {
        "name": "Ioneer Rhyolite Ridge (Lithium-Boron)",
        "area": "Esmeralda County",
        "county": "Esmeralda County",
        "crane_use": "Processing facility construction starting 2025-2026",
        "contact_strategy": "EPC contractor selection in progress — get on bid list now",
        "blm_url": "https://www.blm.gov/nevada",
    },
    {
        "name": "Hecla Mining — Greens Creek",
        "area": "Southeast Alaska / NV exploration",
        "county": "Multiple",
        "crane_use": "Annual maintenance shutdowns, equipment replacement cycles",
        "contact_strategy": "Mine maintenance manager — annual TAR cycle, recurring crane need",
        "blm_url": "https://www.blm.gov",
    },
    {
        "name": "Coeur Mining — Rochester Silver Mine Expansion",
        "area": "Pershing County, NV",
        "county": "Pershing County",
        "crane_use": "Heap leach pad expansion, crusher installation — ACTIVE NOW",
        "contact_strategy": "Coeur just completed $680M expansion — maintenance contracts opening",
        "blm_url": "https://www.blm.gov/nevada",
    },
    {
        "name": "Nevada Copper — Pumpkin Hollow",
        "area": "Lyon County",
        "county": "Lyon County",
        "crane_use": "Underground and open pit development, concentrator facility",
        "contact_strategy": "Call their construction manager — early stage, needs relationships now",
        "blm_url": "https://www.blm.gov/nevada",
    },
]


def scrape_blm_lr2000(state="NV"):
    """
    BLM LR2000 — Mining claims, permits, right-of-way applications.
    Public API: https://www.blm.gov/lr2000
    Real query: https://www.blm.gov/lr2000/EPLF.cfm
    """
    print(f"⛏️  Fetching BLM LR2000 mining data for {state}...")
    leads = []

    # BLM LR2000 Case search API
    url = "https://www.blm.gov/lr2000/CASE_SUMMARY.cfm"
    params = {
        "CASE_TYPE": "MINING",
        "STATE": state,
        "DISP": "Active",
        "START_DATE": (datetime.now() - timedelta(days=365)).strftime("%m/%d/%Y"),
    }

    try:
        r = requests.get(url, params=params, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(r.content, "html.parser")
        tables = soup.find_all("table")

        for table in tables:
            rows = table.find_all("tr")
            for row in rows[1:20]:
                cells = row.find_all("td")
                if len(cells) >= 3:
                    lead = {
                        "source": "BLM_LR2000",
                        "case_id": cells[0].get_text(strip=True),
                        "company": cells[1].get_text(strip=True),
                        "case_type": "Mining Permit",
                        "county": cells[2].get_text(strip=True),
                        "state": state,
                        "status": "Active",
                        "crane_use": "Mining operations — equipment, conveyor, mill maintenance",
                        "action": "Call mine maintenance superintendent about crane needs",
                        "url": url,
                    }
                    leads.append(lead)

        print(f"   BLM LR2000: {len(leads)} mining permits found")
    except Exception as e:
        print(f"   ⚠️  BLM LR2000 error: {e} — using seeded data")

    # Add seeded Nevada mining targets
    for company in NV_MINING_COMPANIES:
        leads.append({
            "source": "NV_MINING_SEED",
            "case_id": "MANUAL_LOOKUP",
            "company": company["name"],
            "case_type": "Active Mining Operation",
            "county": company["county"],
            "state": "NV",
            "status": "Active",
            "crane_use": company["crane_use"],
            "action": company["contact_strategy"],
            "url": company["blm_url"],
        })

    return leads


def scrape_solar_row_applications():
    """
    BLM Right-of-Way applications for solar projects.
    These are filed before construction permits, giving 12-18 months advance notice.
    """
    print("☀️  Fetching BLM Solar ROW applications...")
    leads = []

    # BLM ePlanning solar projects
    url = "https://eplanning.blm.gov/eplanning-ui/project/list"
    params = {
        "projectType": "Right-of-Way",
        "state": "NV,TX,CA,AZ",
        "status": "Active",
    }

    try:
        r = requests.get(url, params=params, headers=HEADERS, timeout=15)
        # Try JSON endpoint
        api_url = "https://eplanning.blm.gov/eplanning-ui/api/project/list?projectType=SOLAR&status=Active&state=NV"
        r2 = requests.get(api_url, headers={**HEADERS, "Accept": "application/json"}, timeout=15)
        if r2.status_code == 200:
            projects = r2.json()
            for proj in (projects if isinstance(projects, list) else []):
                mw = proj.get("capacity_mw", proj.get("megawatts", 0))
                if not mw:
                    mw = 0
                leads.append({
                    "source": "BLM_SOLAR_ROW",
                    "project_name": proj.get("name", proj.get("projectName", "")),
                    "company": proj.get("developer", proj.get("applicant", "")),
                    "county": proj.get("county", ""),
                    "state": proj.get("state", ""),
                    "mw": mw,
                    "status": proj.get("status", "Active"),
                    "crane_use": "Transformer installation, substation construction, inverter placement",
                    "epc_contact": "Identify EPC contractor from BLM filing",
                    "action": f"Find EPC contractor for this project, pitch crane service for transformer/substation work",
                    "url": f"https://eplanning.blm.gov/eplanning-ui/project/{proj.get('id','')}/510",
                })
        print(f"   BLM Solar: {len(leads)} active ROW applications")
    except Exception as e:
        print(f"   ⚠️  BLM Solar API error: {e} — using EPC contact list")

    return leads


def generate_solar_epc_outreach():
    """Generate immediate outreach targets from known solar EPC firms."""
    print("☀️  Generating Solar EPC firm outreach list...")
    leads = []
    for firm in SOLAR_EPC_FIRMS:
        leads.append({
            "source": "SOLAR_EPC_FIRMS",
            "project_name": "Multiple active solar projects",
            "company": firm["name"],
            "county": "Multiple — TX, NV, CA, AZ",
            "state": "Multiple",
            "mw": "Various — 50-500MW per project",
            "status": "Active bidding",
            "crane_use": "Transformer installation, substation construction (20-200 ton cranes), inverter skid placement",
            "epc_contact": f"Call main line: {firm['phone']}. Ask for heavy lift/crane subcontractor coordinator.",
            "action": f"[CALL TODAY] {firm['phone']} — ask for project manager handling current TX/NV solar builds",
            "url": "https://www.solarindustrymag.com/top-epc-contractors",
        })
    return leads


def run():
    print(f"\n{'='*60}")
    print("CraneGenius Mining + Solar Intel Scanner")
    print(f"{'='*60}\n")

    all_leads = []
    all_leads.extend(scrape_blm_lr2000("NV"))
    all_leads.extend(scrape_solar_row_applications())
    all_leads.extend(generate_solar_epc_outreach())

    if all_leads:
        all_keys = set()
        for l in all_leads:
            all_keys.update(l.keys())
        with open(OUTPUT_FILE, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=sorted(all_keys))
            writer.writeheader()
            for lead in all_leads:
                writer.writerow({k: lead.get(k, "") for k in sorted(all_keys)})
        print(f"\n✅ {len(all_leads)} mining/solar leads → {OUTPUT_FILE}")

    print("\nTOP IMMEDIATE TARGETS:")
    for l in all_leads[:12]:
        print(f"  🎯 {l['company']} ({l.get('county','')}, {l.get('state','')})")
        print(f"     Use: {l.get('crane_use','')[:60]}")
        action = l.get('action', l.get('epc_contact', ''))
        print(f"     → {action[:80]}")
        print()

    return all_leads


if __name__ == "__main__":
    run()
