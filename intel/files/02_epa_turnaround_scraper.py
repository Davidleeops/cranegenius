"""
CraneGenius Intel #2: EPA Air Permit Variance Filings — Turnaround Early Warning
Scrapes TCEQ (Texas), IEPA (Illinois), NDEP (Nevada) for planned operational changes
at refineries and industrial facilities. Filed 6-12 months before turnaround.

Run: python 02_epa_variance_scraper.py
"""

import requests
from bs4 import BeautifulSoup
import csv
import time
from datetime import datetime, timedelta
import re

OUTPUT_FILE = "epa_turnaround_leads.csv"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) CraneGenius Research"
}

# Known industrial facilities by state (pre-seeded — add more from EPA FRS)
# EPA FRS API: https://frs.epa.gov/frs_public2/fii/fii_query_detail.cfm
KNOWN_FACILITIES = {
    "TX": [
        {"name": "Valero Houston Refinery", "city": "Houston", "lat": 29.7604, "lng": -95.3698},
        {"name": "LyondellBasell Houston", "city": "Houston", "lat": 29.7749, "lng": -95.4194},
        {"name": "ExxonMobil Baytown", "city": "Baytown", "lat": 29.7355, "lng": -94.9774},
        {"name": "Chevron Phillips Cedar Bayou", "city": "Baytown", "lat": 29.7366, "lng": -94.9660},
        {"name": "Shell Deer Park", "city": "Deer Park", "lat": 29.7052, "lng": -95.1166},
        {"name": "Marathon Galveston Bay Refinery", "city": "Texas City", "lat": 29.3838, "lng": -94.9027},
        {"name": "INEOS Chocolate Bayou", "city": "Alvin", "lat": 29.3727, "lng": -95.2441},
    ],
    "IL": [
        {"name": "BP Whiting Refinery", "city": "Whiting", "lat": 41.6786, "lng": -87.4953},
        {"name": "ExxonMobil Joliet", "city": "Joliet", "lat": 41.5250, "lng": -88.0817},
        {"name": "Citgo Lemont Refinery", "city": "Lemont", "lat": 41.6736, "lng": -88.0006},
    ],
    "NV": [
        {"name": "Barrick Goldstrike Mine", "city": "Elko County", "lat": 40.9068, "lng": -116.3508},
        {"name": "Nevada Copper Pumpkin Hollow", "city": "Yerington", "lat": 38.9885, "lng": -119.1637},
        {"name": "Lithium Nevada Thacker Pass", "city": "Winnemucca", "lat": 41.8286, "lng": -118.0369},
    ],
}


# ── TCEQ SCRAPER ─────────────────────────────────────────────────────────────
def scrape_tceq_notifications():
    """
    TCEQ Central Registry: Air permit notifications and variance requests.
    Real URL: https://www2.tceq.texas.gov/airperm/index.cfm
    We'll query their public search for recent amendments/notifications.
    """
    print("📋 Scanning TCEQ (Texas) variance filings...")
    leads = []

    # TCEQ Air Permits search - recent amendments
    url = "https://www2.tceq.texas.gov/airperm/index.cfm"
    params = {
        "fuseaction": "airperm.airperm_search",
        "activity": "air",
        "permittype": "NSR",
        "beginDate": (datetime.now() - timedelta(days=180)).strftime("%m/%d/%Y"),
        "endDate": datetime.now().strftime("%m/%d/%Y"),
    }

    try:
        r = requests.get(url, params=params, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(r.content, "html.parser")

        # Parse results table
        table = soup.find("table", {"class": "data"})
        if table:
            rows = table.find_all("tr")[1:]  # Skip header
            for row in rows[:50]:
                cells = row.find_all("td")
                if len(cells) >= 4:
                    lead = {
                        "source": "TCEQ",
                        "permit_num": cells[0].get_text(strip=True),
                        "company": cells[1].get_text(strip=True),
                        "city": cells[2].get_text(strip=True),
                        "county": cells[3].get_text(strip=True),
                        "filing_type": "Air Permit Amendment",
                        "filed_date": cells[4].get_text(strip=True) if len(cells) > 4 else "",
                        "crane_relevance": "TURNAROUND",
                        "estimated_work_window": "6-12 months from filing",
                        "action": "Call facility manager, ask about planned maintenance contractor",
                        "url": f"https://www2.tceq.texas.gov/airperm/index.cfm",
                    }
                    leads.append(lead)
        print(f"   ✅ Found {len(leads)} TCEQ filings")
    except Exception as e:
        print(f"   ⚠️  TCEQ error: {e} — using facility seed list")
        # Fall back to known facilities list as starting point
        for facility in KNOWN_FACILITIES.get("TX", []):
            leads.append({
                "source": "TCEQ_SEED",
                "permit_num": "LOOKUP_REQUIRED",
                "company": facility["name"],
                "city": facility["city"],
                "county": "TX",
                "filing_type": "Known Industrial Facility",
                "filed_date": datetime.now().strftime("%Y-%m-%d"),
                "crane_relevance": "TURNAROUND_CANDIDATE",
                "estimated_work_window": "Research current TAR schedule",
                "action": f"Search TCEQ: https://www2.tceq.texas.gov/airperm/index.cfm for {facility['name']}",
                "url": "https://www2.tceq.texas.gov/airperm/index.cfm",
            })

    return leads


# ── EPA FRS FACILITY SCRAPER ──────────────────────────────────────────────────
def scrape_epa_frs(state_code):
    """
    EPA Facility Registry Service — find all industrial facilities in a state.
    Then cross-reference against permit activity.
    API: https://ofmpub.epa.gov/frs_public2/fii_query_detail.cfm
    """
    print(f"🏭 Fetching EPA FRS industrial facilities for {state_code}...")
    leads = []

    # EPA FRS REST API
    url = "https://ofmpub.epa.gov/frs_public2/frs_rest_services.get_facilities"
    params = {
        "state_abbr": state_code,
        "facility_type_code": "REFINERY,CHEMICAL,POWER",
        "output": "JSON",
        "p_rows": "100",
    }
    try:
        r = requests.get(url, params=params, headers=HEADERS, timeout=15)
        data = r.json()
        facilities = data.get("Results", {}).get("FRSFacility", [])
        for f in facilities:
            leads.append({
                "source": "EPA_FRS",
                "facility_id": f.get("REGISTRY_ID", ""),
                "company": f.get("PRIMARY_NAME", ""),
                "city": f.get("CITY_NAME", ""),
                "county": f.get("COUNTY_NAME", ""),
                "state": state_code,
                "naics": f.get("NAICS_CODE", ""),
                "crane_relevance": "INDUSTRIAL_FACILITY",
                "lat": f.get("LATITUDE83", ""),
                "lng": f.get("LONGITUDE83", ""),
                "action": "Research permit activity, call plant manager about TAR schedule",
                "url": f"https://ofmpub.epa.gov/frs_public2/fii_query_detail.cfm?facility_id={f.get('REGISTRY_ID','')}",
            })
        print(f"   ✅ Found {len(leads)} EPA FRS facilities in {state_code}")
    except Exception as e:
        print(f"   ⚠️  EPA FRS error: {e}")

    return leads


# ── MAIN ─────────────────────────────────────────────────────────────────────
def run():
    print(f"\n{'='*60}")
    print("CraneGenius EPA/Turnaround Intel Scanner")
    print(f"{'='*60}\n")

    all_leads = []

    # TCEQ Texas
    all_leads.extend(scrape_tceq_notifications())

    # EPA FRS for target states
    for state in ["TX", "IL", "NV", "CA"]:
        all_leads.extend(scrape_epa_frs(state))
        time.sleep(1)

    # De-dupe by company name
    seen = set()
    unique_leads = []
    for lead in all_leads:
        key = lead.get("company", "").lower()[:30]
        if key not in seen:
            seen.add(key)
            unique_leads.append(lead)

    if unique_leads:
        with open(OUTPUT_FILE, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=unique_leads[0].keys())
            writer.writeheader()
            writer.writerows(unique_leads)
        print(f"\n✅ {len(unique_leads)} turnaround candidates → {OUTPUT_FILE}")

    print("\nTOP TURNAROUND TARGETS:")
    for lead in unique_leads[:10]:
        print(f"  🔧 {lead['company']} ({lead.get('city','')}, {lead.get('state',lead.get('county',''))})")
        print(f"     → {lead['action']}")

    return unique_leads


if __name__ == "__main__":
    run()
