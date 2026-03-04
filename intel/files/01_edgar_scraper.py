"""
CraneGenius Intel #1: SEC EDGAR Full-Text Search
Finds public companies announcing turnarounds, plant expansions, capital projects
Gives 12-18 months advance notice before any permit is filed
Run: python 01_edgar_scraper.py
"""

import requests
import json
import csv
import time
from datetime import datetime, timedelta

# ── CONFIG ──────────────────────────────────────────────────────────────────
SEARCH_TERMS = [
    "planned turnaround",
    "scheduled turnaround",
    "planned shutdown",
    "scheduled outage",
    "capital expansion",
    "new facility construction",
    "plant expansion",
    "turnaround maintenance",
    "major maintenance",
    "planned outage",
]

# Industrial SIC codes — only companies likely to need cranes
INDUSTRIAL_SIC = [
    "2911",  # Petroleum Refining
    "2819",  # Industrial Inorganic Chemicals
    "2821",  # Plastics Materials
    "2860",  # Industrial Chemicals
    "3317",  # Steel Pipe & Tubes
    "3312",  # Steel Works, Blast Furnaces
    "3559",  # Special Industry Machinery
    "3669",  # Communications Equipment
    "3490",  # Metal Services
    "1311",  # Crude Petroleum & Natural Gas
    "1381",  # Drilling Oil & Gas Wells
    "4911",  # Electric Services (utilities)
    "4924",  # Natural Gas Distribution
    "4941",  # Water Supply
]

# Days back to search
DAYS_BACK = 90
FORMS = ["10-Q", "10-K", "8-K"]
OUTPUT_FILE = "edgar_crane_leads.csv"

HEADERS = {"User-Agent": "CraneGenius research@cranegenius.com"}

# ── EDGAR FULL TEXT SEARCH ───────────────────────────────────────────────────
def search_edgar(query, start_date, end_date, forms="10-Q,10-K"):
    url = "https://efts.sec.gov/LATEST/search-index"
    params = {
        "q": f'"{query}"',
        "dateRange": "custom",
        "startdt": start_date,
        "enddt": end_date,
        "forms": forms,
    }
    try:
        r = requests.get(url, params=params, headers=HEADERS, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"  Error searching '{query}': {e}")
        return {}


def get_filing_details(accession_no, cik):
    """Fetch actual filing text snippet to extract location/project details."""
    url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_no.replace('-','')}/{accession_no}-index.json"
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        return r.json()
    except:
        return {}


def get_company_info(cik):
    """Get company name, SIC, state from EDGAR company facts."""
    url = f"https://data.sec.gov/submissions/CIK{str(cik).zfill(10)}.json"
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        data = r.json()
        return {
            "name": data.get("name", ""),
            "sic": data.get("sic", ""),
            "sic_desc": data.get("sicDescription", ""),
            "state": data.get("stateOfIncorporation", ""),
            "city": data.get("addresses", {}).get("business", {}).get("city", ""),
            "state_loc": data.get("addresses", {}).get("business", {}).get("stateOrCountry", ""),
        }
    except:
        return {}


# ── MAIN ─────────────────────────────────────────────────────────────────────
def run():
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=DAYS_BACK)).strftime("%Y-%m-%d")

    print(f"\n{'='*60}")
    print(f"CraneGenius EDGAR Intel Scanner")
    print(f"Searching {start_date} → {end_date}")
    print(f"Terms: {len(SEARCH_TERMS)} | Forms: {FORMS}")
    print(f"{'='*60}\n")

    leads = []
    seen = set()

    for term in SEARCH_TERMS:
        print(f"🔍 Searching: \"{term}\"...")
        results = search_edgar(term, start_date, end_date)
        hits = results.get("hits", {}).get("hits", [])
        print(f"   Found {len(hits)} filings")

        for hit in hits[:20]:  # Top 20 per term
            source = hit.get("_source", {})
            entity_id = source.get("entity_id", "")
            accession = source.get("file_date", "")
            filing_key = f"{entity_id}_{source.get('period_of_report','')}"

            if filing_key in seen:
                continue
            seen.add(filing_key)

            company = get_company_info(entity_id)
            sic = company.get("sic", "")

            # Filter to industrial companies
            if sic and sic not in INDUSTRIAL_SIC:
                continue

            lead = {
                "company": company.get("name", source.get("entity_name", "")),
                "cik": entity_id,
                "sic_code": sic,
                "sic_desc": company.get("sic_desc", ""),
                "city": company.get("city", ""),
                "state": company.get("state_loc", ""),
                "form_type": source.get("form_type", ""),
                "filing_date": source.get("file_date", ""),
                "period": source.get("period_of_report", ""),
                "trigger_term": term,
                "edgar_url": f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={entity_id}&type={source.get('form_type','')}&dateb=&owner=include&count=5",
                "estimated_project_start": "6-18 months from filing",
                "priority": "HIGH" if sic in ["2911", "1311", "4911"] else "MEDIUM",
            }
            leads.append(lead)
            time.sleep(0.1)  # Rate limit

        time.sleep(0.5)

    # Sort by priority
    leads.sort(key=lambda x: (x["priority"] == "MEDIUM", x["filing_date"]), reverse=False)

    # Write CSV
    if leads:
        with open(OUTPUT_FILE, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=leads[0].keys())
            writer.writeheader()
            writer.writerows(leads)
        print(f"\n✅ {len(leads)} leads written to {OUTPUT_FILE}")
    else:
        print("\n⚠️  No leads found — check internet connection and date range")

    # Print preview
    print(f"\n{'─'*60}")
    print("TOP LEADS PREVIEW:")
    for l in leads[:10]:
        print(f"  [{l['priority']}] {l['company']} ({l['city']}, {l['state']}) — {l['form_type']} filed {l['filing_date']}")
        print(f"         Trigger: \"{l['trigger_term']}\" | SIC: {l['sic_desc']}")

    return leads


if __name__ == "__main__":
    run()
