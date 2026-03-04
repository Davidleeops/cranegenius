"""
CraneGenius Intel #4: Job Posting Intelligence — Intent Signals
Hiring a Turnaround Coordinator = live signal of planned industrial maintenance event.
Hiring a Construction Project Manager (Industrial) = expansion underway.

Sources: Indeed public search (no login required), plus optional Apify API
These roles are hired 6-12 months before the event.

Run: python 04_job_posting_scraper.py
"""

import requests
from bs4 import BeautifulSoup
import csv
import time
import re
from datetime import datetime
from urllib.parse import urlencode, quote_plus

OUTPUT_FILE = "job_posting_leads.csv"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

# Job titles that signal crane-relevant events
SIGNAL_TITLES = [
    "turnaround coordinator",
    "TAR planner",
    "shutdown manager",
    "turnaround manager",
    "plant turnaround",
    "maintenance turnaround",
    "construction project manager industrial",
    "heavy lift supervisor",
    "rigging supervisor",
    "plant expansion project manager",
    "capital projects manager",
]

# Target locations (city, state)
TARGET_MARKETS = [
    ("Houston", "TX"),
    ("Baytown", "TX"),
    ("Texas City", "TX"),
    ("Beaumont", "TX"),
    ("Port Arthur", "TX"),
    ("Chicago", "IL"),
    ("Joliet", "IL"),
    ("Whiting", "IN"),
    ("Reno", "NV"),
    ("Sparks", "NV"),
    ("Las Vegas", "NV"),
    ("Dallas", "TX"),
    ("Midland", "TX"),
]

# Industry keywords that appear in job descriptions — signals crane need
INDUSTRY_KEYWORDS = [
    "refinery", "petrochemical", "chemical plant", "LNG", "gas plant",
    "industrial", "manufacturing", "power plant", "data center",
    "substation", "transmission", "mining", "oil and gas",
]


def scrape_indeed(title, location, state):
    """Scrape Indeed public job listings."""
    jobs = []
    query = quote_plus(title)
    loc = quote_plus(f"{location}, {state}")

    url = f"https://www.indeed.com/jobs?q={query}&l={loc}&fromage=30&sort=date"

    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            return jobs

        soup = BeautifulSoup(r.content, "html.parser")

        # Indeed job cards
        job_cards = soup.find_all("div", {"class": re.compile(r"job_seen_beacon|tapItem")})

        for card in job_cards[:10]:
            title_el = card.find(["h2", "span"], {"class": re.compile(r"jobTitle|title")})
            company_el = card.find(["span", "div"], {"class": re.compile(r"companyName|company")})
            location_el = card.find(["div", "span"], {"class": re.compile(r"companyLocation|location")})
            date_el = card.find(["span"], {"class": re.compile(r"date|posted")})

            job_title = title_el.get_text(strip=True) if title_el else ""
            company = company_el.get_text(strip=True) if company_el else ""
            job_location = location_el.get_text(strip=True) if location_el else location
            posted = date_el.get_text(strip=True) if date_el else ""

            # Get job link
            link_el = card.find("a", href=re.compile(r"/rc/clk|/pagead"))
            job_url = ""
            if link_el:
                href = link_el.get("href", "")
                job_url = f"https://www.indeed.com{href}" if href.startswith("/") else href

            if not company:
                continue

            # Score by industry relevance
            description_text = card.get_text().lower()
            industry_score = sum(1 for kw in INDUSTRY_KEYWORDS if kw in description_text)

            jobs.append({
                "source": "Indeed",
                "search_title": title,
                "job_title": job_title,
                "company": company,
                "location": job_location,
                "market": f"{location}, {state}",
                "posted": posted,
                "industry_score": industry_score,
                "crane_event": infer_crane_event(title),
                "estimated_event_timeline": "6-12 months from posting",
                "priority": "HIGH" if industry_score >= 2 else "MEDIUM",
                "action": f"Research {company} facility, call plant manager about TAR/expansion crane needs",
                "url": job_url,
                "scraped_date": datetime.now().strftime("%Y-%m-%d"),
            })

        time.sleep(2)  # Respect rate limits
    except Exception as e:
        print(f"    Indeed error for '{title}' in {location}: {e}")

    return jobs


def scrape_linkedin_public(title, location):
    """
    LinkedIn public job search — no login required for basic results.
    More reliable with Apify LinkedIn scraper (paid) or Bright Data.
    """
    jobs = []
    query = quote_plus(title)
    loc = quote_plus(location)

    url = f"https://www.linkedin.com/jobs/search/?keywords={query}&location={loc}&f_TPR=r2592000&sortBy=DD"

    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(r.content, "html.parser")

        job_cards = soup.find_all("div", {"class": re.compile(r"base-card|job-search-card")})

        for card in job_cards[:5]:
            title_el = card.find(["h3", "span"], {"class": re.compile(r"base-search-card__title|job-search-card__title")})
            company_el = card.find(["h4", "a"], {"class": re.compile(r"base-search-card__subtitle|job-search-card__company-name")})
            location_el = card.find(["span"], {"class": re.compile(r"job-search-card__location")})

            job_title = title_el.get_text(strip=True) if title_el else ""
            company = company_el.get_text(strip=True) if company_el else ""
            job_location = location_el.get_text(strip=True) if location_el else location

            if not company:
                continue

            jobs.append({
                "source": "LinkedIn",
                "search_title": title,
                "job_title": job_title,
                "company": company,
                "location": job_location,
                "market": location,
                "posted": "< 30 days",
                "industry_score": 1,
                "crane_event": infer_crane_event(title),
                "estimated_event_timeline": "6-12 months from posting",
                "priority": "HIGH",
                "action": f"Find {company} plant manager on LinkedIn, send specific outreach about crane availability for planned event",
                "url": url,
                "scraped_date": datetime.now().strftime("%Y-%m-%d"),
            })

        time.sleep(3)
    except Exception as e:
        print(f"    LinkedIn error for '{title}' in {location}: {e}")

    return jobs


def infer_crane_event(job_title):
    """Map job title to likely crane event type."""
    title = job_title.lower()
    if "turnaround" in title or "tar" in title or "shutdown" in title:
        return "PLANNED_TURNAROUND"
    elif "capital" in title or "expansion" in title:
        return "PLANT_EXPANSION"
    elif "construction" in title:
        return "NEW_CONSTRUCTION"
    elif "maintenance" in title:
        return "SCHEDULED_MAINTENANCE"
    return "INDUSTRIAL_EVENT"


def run():
    print(f"\n{'='*60}")
    print("CraneGenius Job Posting Intent Scanner")
    print(f"{'='*60}\n")

    all_jobs = []
    total_searches = len(SIGNAL_TITLES) * len(TARGET_MARKETS)
    count = 0

    # Focus on highest-value title/market combos first
    priority_titles = SIGNAL_TITLES[:5]
    priority_markets = [m for m in TARGET_MARKETS if m[1] in ["TX", "IL", "NV"]][:6]

    for title in priority_titles:
        for location, state in priority_markets:
            count += 1
            print(f"🔍 [{count}/{len(priority_titles)*len(priority_markets)}] '{title}' in {location}, {state}...")

            # Try Indeed first (more permissive)
            jobs = scrape_indeed(title, location, state)
            all_jobs.extend(jobs)

            # LinkedIn as secondary
            if len(jobs) < 3:
                linkedin_jobs = scrape_linkedin_public(title, f"{location}, {state}")
                all_jobs.extend(linkedin_jobs)

            if jobs or linkedin_jobs:
                print(f"   ✅ Found {len(jobs)} Indeed + LinkedIn results")
            else:
                print(f"   — No results (may need Apify for this market)")

    # De-dupe by company+location
    seen = set()
    unique_jobs = []
    for job in all_jobs:
        key = f"{job['company'].lower()[:20]}_{job['market'].lower()[:15]}"
        if key not in seen:
            seen.add(key)
            unique_jobs.append(job)

    # Sort by priority
    unique_jobs.sort(key=lambda x: (x["priority"] != "HIGH", -x.get("industry_score", 0)))

    if unique_jobs:
        with open(OUTPUT_FILE, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=unique_jobs[0].keys())
            writer.writeheader()
            writer.writerows(unique_jobs)
        print(f"\n✅ {len(unique_jobs)} job posting leads → {OUTPUT_FILE}")
    else:
        print("\n⚠️  No results — scrapers may be blocked. Try Apify LinkedIn Actor or use manual search.")
        print("   Manual search URL: https://www.indeed.com/jobs?q=turnaround+coordinator&l=Houston,+TX&fromage=30")

    print("\nIF SCRAPERS ARE BLOCKED — use these manual search URLs:")
    for title in priority_titles[:3]:
        encoded = quote_plus(title)
        print(f"  Indeed: https://www.indeed.com/jobs?q={encoded}&l=Houston%2C+TX&fromage=30")

    print("\nRECOMMENDED APIFY ACTORS (paid, reliable):")
    print("  LinkedIn Job Scraper: https://apify.com/curious_coder/linkedin-jobs-scraper")
    print("  Indeed Scraper: https://apify.com/misceres/indeed-scraper")
    print("  Cost: ~$10-20/month for weekly runs")

    return unique_jobs


if __name__ == "__main__":
    run()
