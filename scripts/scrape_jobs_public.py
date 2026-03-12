#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List
from urllib.parse import quote_plus
from urllib.request import Request, urlopen

DEFAULT_KEYWORDS = [
    "crane operator",
    "tower crane operator",
    "rigger",
    "heavy equipment operator",
    "hoist operator",
]

CRANE_ROLE_RE = re.compile(
    r"\b(crane|rigger|lift director|signalperson|hoist|heavy equipment operator|mobile operator|crawler|rigging)\b",
    re.IGNORECASE,
)

FALLBACK_SEED_JOBS = [
    {"title": "Mobile Crane Operator (AT)", "company": "Regional Lift Services", "location": "Houston, TX", "type": "Full-Time", "description": "Operate 90T-220T all-terrain cranes on commercial projects. NCCCO required."},
    {"title": "Tower Crane Operator", "company": "Urban Highrise Constructors", "location": "New York, NY", "type": "Full-Time", "description": "Run tower crane for high-rise core/shell operations with strict lift plans."},
    {"title": "Rigger / Signalperson", "company": "Precision Rigging Group", "location": "Chicago, IL", "type": "Full-Time", "description": "Field rigging, signaling, and lift prep for steel and mechanical picks."},
    {"title": "Crawler Crane Operator", "company": "Gulf Coast Heavy Lift", "location": "Baton Rouge, LA", "type": "Contract", "description": "Operate crawler cranes for plant turnaround and module placement."},
    {"title": "Hydraulic Truck Crane Operator", "company": "Metro Crane Rental", "location": "Dallas, TX", "type": "Full-Time", "description": "Daily taxi-crane work across construction and industrial sites."},
    {"title": "Hoist Operator", "company": "BuildCore Contractors", "location": "Miami, FL", "type": "Full-Time", "description": "Operate personnel/material hoists and support vertical logistics planning."},
    {"title": "Heavy Equipment Operator (Crane Focus)", "company": "Northline Civil", "location": "Phoenix, AZ", "type": "Full-Time", "description": "Operate cranes and support equipment for civil and utility projects."},
    {"title": "Lift Director", "company": "Atlas Lift Planning", "location": "Atlanta, GA", "type": "Full-Time", "description": "Lead engineered lift plans, pre-task briefings, and critical pick execution."},
    {"title": "Crane Apprentice", "company": "Skyline Operators Union Partner", "location": "Seattle, WA", "type": "Apprenticeship", "description": "Entry track for crane operations with mentored field rotations."},
    {"title": "Wind Project Crane Operator", "company": "Renewable Build Partners", "location": "Des Moines, IA", "type": "Contract", "description": "Operate large crawlers and assist wind component installation teams."},
    {"title": "NCCCO Certified Operator", "company": "Frontier Lift Logistics", "location": "Nashville, TN", "type": "Full-Time", "description": "Certified crane operations for mixed commercial and industrial accounts."},
    {"title": "Rigging Foreman", "company": "Summit Industrial Services", "location": "Pittsburgh, PA", "type": "Full-Time", "description": "Supervise rigging crews and execute complex lift sequences safely."},
]


def fetch_bytes(url: str, headers: Dict[str, str] | None = None, timeout: int = 25) -> bytes:
    req = Request(url, headers=headers or {})
    with urlopen(req, timeout=timeout) as resp:
        return resp.read()


def fetch_json(url: str, headers: Dict[str, str] | None = None, timeout: int = 25) -> Dict:
    raw = fetch_bytes(url, headers=headers, timeout=timeout)
    return json.loads(raw.decode("utf-8", errors="ignore"))


def normalize_text(value: object) -> str:
    return str(value or "").strip()


def parse_manual_import(path: Path, limit: int) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    out: List[Dict[str, str]] = []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            title = normalize_text(row.get("title"))
            company = normalize_text(row.get("company"))
            if not title or not company:
                continue
            out.append(
                {
                    "title": title,
                    "company": company,
                    "location": normalize_text(row.get("location")) or "USA",
                    "type": normalize_text(row.get("type")) or "Full-Time",
                    "description": normalize_text(row.get("description"))[:700],
                    "source": normalize_text(row.get("source")) or "manual_import_csv",
                    "source_url": normalize_text(row.get("source_url")),
                    "posted_at": normalize_text(row.get("posted_at")) or datetime.now(timezone.utc).isoformat(),
                }
            )
            if len(out) >= limit:
                break
    return out


def parse_remotive(keyword: str, limit: int) -> List[Dict[str, str]]:
    url = f"https://remotive.com/api/remote-jobs?search={quote_plus(keyword)}"
    data = fetch_json(url)
    out: List[Dict[str, str]] = []
    for row in data.get("jobs", [])[:limit]:
        title = normalize_text(row.get("title"))
        company = normalize_text(row.get("company_name"))
        if not title or not company:
            continue
        out.append(
            {
                "title": title,
                "company": company,
                "location": normalize_text(row.get("candidate_required_location")) or "Remote",
                "type": normalize_text(row.get("job_type")) or "Contract",
                "description": normalize_text(row.get("description"))[:700],
                "source": "remotive_api",
                "source_url": normalize_text(row.get("url")),
                "posted_at": normalize_text(row.get("publication_date")),
            }
        )
    return out


def parse_usajobs(keyword: str, limit: int, source_name: str) -> List[Dict[str, str]]:
    api_key = normalize_text(os.getenv("USAJOBS_API_KEY"))
    user_email = normalize_text(os.getenv("USAJOBS_USER_EMAIL"))
    if not api_key or not user_email:
        raise RuntimeError("USAJOBS_API_KEY and USAJOBS_USER_EMAIL are required for USAJobs API")

    url = f"https://data.usajobs.gov/api/search?Keyword={quote_plus(keyword)}&ResultsPerPage={min(max(limit,1),250)}"
    headers = {
        "Host": "data.usajobs.gov",
        "User-Agent": user_email,
        "Authorization-Key": api_key,
    }
    data = fetch_json(url, headers=headers)

    out: List[Dict[str, str]] = []
    items = data.get("SearchResult", {}).get("SearchResultItems", [])
    for item in items[:limit]:
        desc = item.get("MatchedObjectDescriptor", {})
        title = normalize_text(desc.get("PositionTitle"))
        org = normalize_text(desc.get("OrganizationName"))
        if not title or not org:
            continue
        locs = desc.get("PositionLocationDisplay", [])
        loc = normalize_text(locs[0] if isinstance(locs, list) and locs else desc.get("PositionLocation")) or "USA"
        out.append(
            {
                "title": title,
                "company": org,
                "location": loc,
                "type": "Full-Time",
                "description": normalize_text(desc.get("QualificationSummary") or "")[:700],
                "source": source_name,
                "source_url": normalize_text(desc.get("PositionURI")),
                "posted_at": normalize_text(desc.get("PublicationStartDate")),
            }
        )
    return out


def parse_arbeitnow(keyword: str, limit: int) -> List[Dict[str, str]]:
    data = fetch_json("https://www.arbeitnow.com/api/job-board-api")
    out: List[Dict[str, str]] = []
    for row in data.get("data", [])[: max(limit * 3, 200)]:
        title = normalize_text(row.get("title"))
        company = normalize_text(row.get("company_name"))
        if not title or not company:
            continue
        combined = f"{title} {normalize_text(row.get('description'))}"
        if keyword and keyword.lower() not in combined.lower() and not CRANE_ROLE_RE.search(combined):
            continue
        out.append(
            {
                "title": title,
                "company": company,
                "location": normalize_text(row.get("location")) or "Remote",
                "type": "Full-Time",
                "description": normalize_text(row.get("description"))[:700],
                "source": "arbeitnow_api",
                "source_url": normalize_text(row.get("url")),
                "posted_at": normalize_text(row.get("created_at")),
            }
        )
        if len(out) >= limit:
            break
    return out


def parse_themuse(keyword: str, limit: int) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    page = 1
    while len(out) < limit and page <= 8:
        url = f"https://www.themuse.com/api/public/jobs?page={page}&descending=true"
        data = fetch_json(url)
        results = data.get("results", [])
        if not results:
            break
        for row in results:
            title = normalize_text(row.get("name"))
            company = normalize_text((row.get("company") or {}).get("name"))
            if not title or not company:
                continue
            desc = normalize_text(row.get("contents"))
            combined = f"{title} {desc}"
            if keyword and keyword.lower() not in combined.lower() and not CRANE_ROLE_RE.search(combined):
                continue
            locations = row.get("locations") or []
            loc = normalize_text(locations[0].get("name") if locations else "USA")
            refs = row.get("refs") or {}
            out.append(
                {
                    "title": title,
                    "company": company,
                    "location": loc,
                    "type": "Full-Time",
                    "description": desc[:700],
                    "source": "themuse_api",
                    "source_url": normalize_text(refs.get("landing_page")),
                    "posted_at": normalize_text(row.get("publication_date")),
                }
            )
            if len(out) >= limit:
                break
        page += 1
    return out


def parse_rss(url: str, source_name: str, limit: int) -> List[Dict[str, str]]:
    raw = fetch_bytes(url, headers={"User-Agent": "cranegenius-jobs-rss"})
    root = ET.fromstring(raw)

    out: List[Dict[str, str]] = []
    items = root.findall(".//item")
    for item in items[:limit]:
        title = normalize_text(item.findtext("title"))
        link = normalize_text(item.findtext("link"))
        desc = normalize_text(item.findtext("description"))
        pub = normalize_text(item.findtext("pubDate"))
        if not title:
            continue

        parts = [p.strip() for p in title.split(" - ") if p.strip()]
        job_title = parts[0] if parts else title
        company = parts[1] if len(parts) > 1 else source_name
        location = parts[2] if len(parts) > 2 else "USA"

        out.append(
            {
                "title": job_title,
                "company": company,
                "location": location,
                "type": "Full-Time",
                "description": desc[:700],
                "source": source_name,
                "source_url": link,
                "posted_at": pub,
            }
        )
    return out


def looks_like_crane_role(title: str, desc: str) -> bool:
    return bool(CRANE_ROLE_RE.search(f"{title} {desc}"))


def dedupe(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    seen = set()
    out = []
    for row in rows:
        key = (
            normalize_text(row.get("title", "")).lower(),
            normalize_text(row.get("company", "")).lower(),
            normalize_text(row.get("location", "")).lower(),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def load_previous_jobs(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    try:
        old = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if isinstance(old, dict) and isinstance(old.get("jobs"), list):
        return old["jobs"]
    return []


def build_seed_jobs(limit: int) -> List[Dict[str, str]]:
    now = datetime.now(timezone.utc).isoformat()
    out = []
    for row in FALLBACK_SEED_JOBS[: max(limit, 1)]:
        item = dict(row)
        item["source"] = "seed_catalog_local"
        item["source_url"] = ""
        item["posted_at"] = now
        out.append(item)
    return out


def scrape_source(source_name: str, keyword: str, limit: int, manual_import_path: Path) -> List[Dict[str, str]]:
    if source_name == "manual_import_csv":
        return parse_manual_import(manual_import_path, limit)
    if source_name == "remotive_api":
        return parse_remotive(keyword, limit)
    if source_name == "usajobs_api_crane":
        return parse_usajobs("crane operator", limit, source_name)
    if source_name == "usajobs_api_rigger":
        return parse_usajobs("rigger", limit, source_name)
    if source_name == "usajobs_api_heavy":
        return parse_usajobs("heavy equipment operator", limit, source_name)
    if source_name == "arbeitnow_api":
        return parse_arbeitnow(keyword, limit)
    if source_name == "themuse_api":
        return parse_themuse(keyword, limit)
    if source_name == "indeed_rss":
        return parse_rss(f"https://www.indeed.com/rss?q={quote_plus(keyword)}&l=United+States", source_name, limit)
    if source_name == "careerjet_rss":
        return parse_rss(f"https://www.careerjet.com/search/rss?l=usa&s={quote_plus(keyword)}", source_name, limit)
    if source_name == "craigslist_ny_rss":
        return parse_rss(f"https://newyork.craigslist.org/search/jjj?query={quote_plus(keyword)}&format=rss", source_name, limit)
    if source_name == "craigslist_la_rss":
        return parse_rss(f"https://losangeles.craigslist.org/search/jjj?query={quote_plus(keyword)}&format=rss", source_name, limit)
    if source_name == "craigslist_chicago_rss":
        return parse_rss(f"https://chicago.craigslist.org/search/jjj?query={quote_plus(keyword)}&format=rss", source_name, limit)
    if source_name == "craigslist_houston_rss":
        return parse_rss(f"https://houston.craigslist.org/search/jjj?query={quote_plus(keyword)}&format=rss", source_name, limit)
    return []


def main() -> int:
    parser = argparse.ArgumentParser(description="Scrape public job feeds and write normalized crane jobs for the Jobs page.")
    parser.add_argument("--keyword", default="", help="Primary search keyword (optional; defaults to multi-keyword sweep)")
    parser.add_argument("--limit", type=int, default=150, help="Max records per source")
    parser.add_argument("--output", default="data/jobs_imported.json", help="Output JSON path")
    parser.add_argument("--manual-import", default="data/jobs_manual_import.csv", help="Optional manual CSV import path")
    args = parser.parse_args()

    manual_import_path = Path(args.manual_import)
    if not manual_import_path.is_absolute():
        manual_import_path = Path.cwd() / manual_import_path

    keywords = [args.keyword.strip()] if args.keyword.strip() else list(DEFAULT_KEYWORDS)
    sources = [
        "manual_import_csv",
        "remotive_api",
        "usajobs_api_crane",
        "usajobs_api_rigger",
        "usajobs_api_heavy",
        "arbeitnow_api",
        "themuse_api",
        "indeed_rss",
        "careerjet_rss",
        "craigslist_ny_rss",
        "craigslist_la_rss",
        "craigslist_chicago_rss",
        "craigslist_houston_rss",
    ]

    rows: List[Dict[str, str]] = []
    source_counts: Dict[str, int] = {}
    source_errors: Dict[str, List[str]] = {}

    for source in sources:
        source_total = 0
        errs: List[str] = []
        source_keywords = [""] if source == "manual_import_csv" else keywords
        for kw in source_keywords:
            try:
                items = scrape_source(source, kw, args.limit, manual_import_path)
            except Exception as e:
                items = []
                label = kw or "manual"
                errs.append(f"{label}: {e.__class__.__name__}: {e}")
            if not items:
                continue
            rows.extend(items)
            source_total += len(items)
        source_counts[source] = source_total
        if errs:
            source_errors[source] = errs[:8]

    filtered = [r for r in rows if looks_like_crane_role(r.get("title", ""), r.get("description", ""))]
    cleaned = dedupe(filtered)

    out_path = Path(args.output)
    if not out_path.is_absolute():
        out_path = Path.cwd() / out_path
    out_path.parent.mkdir(parents=True, exist_ok=True)

    previous_jobs = load_previous_jobs(out_path)
    used_previous = False
    used_seed_catalog = False

    if not cleaned and previous_jobs:
        cleaned = previous_jobs
        used_previous = True

    if not cleaned:
        cleaned = build_seed_jobs(limit=min(25, max(args.limit, 1)))
        used_seed_catalog = True

    # Keep manual/live rows, but maintain a minimum baseline population.
    min_baseline = min(12, max(args.limit, 1))
    if len(cleaned) < min_baseline:
        seed = build_seed_jobs(limit=min_baseline)
        seen = {(normalize_text(r.get("title")).lower(), normalize_text(r.get("company")).lower(), normalize_text(r.get("location")).lower()) for r in cleaned}
        for r in seed:
            key = (normalize_text(r.get("title")).lower(), normalize_text(r.get("company")).lower(), normalize_text(r.get("location")).lower())
            if key in seen:
                continue
            cleaned.append(r)
            seen.add(key)
            used_seed_catalog = True
            if len(cleaned) >= min_baseline:
                break

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "keywords": keywords,
        "manual_import_path": str(manual_import_path),
        "sources_attempted": sources,
        "source_counts_raw": source_counts,
        "source_errors": source_errors,
        "raw_rows": len(rows),
        "filtered_rows": len(filtered),
        "count": len(cleaned),
        "jobs": cleaned,
        "used_previous_output": used_previous,
        "used_seed_catalog": used_seed_catalog,
    }

    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    msg = f"Wrote {len(cleaned)} jobs to {out_path}"
    if used_previous:
        msg += " (preserved previous non-empty output)"
    if used_seed_catalog:
        msg += " (seed catalog fallback)"
    print(msg)
    print(f"Sources attempted: {len(sources)}")
    print("Source raw counts:", source_counts)
    if source_errors:
        print("Source errors captured:")
        for name, vals in source_errors.items():
            print(f"  {name}: {vals[0]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
