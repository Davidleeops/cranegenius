"""
CraneGenius Intel Suite — Master Orchestrator
Runs all scrapers, deduplicates, scores, and produces a unified lead report.

Usage:
  python run_all.py                    # Run all scrapers
  python run_all.py --sources edgar    # Run specific scraper
  python run_all.py --email            # Also send email digest

Schedule with cron for weekly runs:
  0 6 * * 1 cd /path/to/cranegenius-intel && python run_all.py
"""

import subprocess
import csv
import os
import sys
import json
from datetime import datetime
from pathlib import Path

SCRAPERS = {
    "edgar":       ("01_edgar_scraper.py",            "SEC Filings — Turnaround/Expansion Signals"),
    "epa":         ("02_epa_turnaround_scraper.py",   "EPA Variance Filings — Turnaround Early Warning"),
    "utility":     ("03_utility_interconnection_scraper.py", "Utility Queue — Data Centers/Industrial Plants"),
    "jobs":        ("04_job_posting_scraper.py",       "Job Postings — Intent Signals"),
    "blm":         ("05_blm_mining_solar_scraper.py",  "BLM Mining + Solar EPC Targets"),
}

OUTPUT_FILES = {
    "edgar":   "edgar_crane_leads.csv",
    "epa":     "epa_turnaround_leads.csv",
    "utility": "utility_interconnection_leads.csv",
    "jobs":    "job_posting_leads.csv",
    "blm":     "blm_mining_solar_leads.csv",
}

MASTER_OUTPUT = f"cranegenius_leads_{datetime.now().strftime('%Y%m%d')}.csv"
SUMMARY_OUTPUT = f"cranegenius_summary_{datetime.now().strftime('%Y%m%d')}.txt"


def run_scraper(key, script):
    """Run a single scraper and return success status."""
    print(f"\n{'─'*60}")
    print(f"▶ Running: {SCRAPERS[key][1]}")
    print(f"{'─'*60}")
    
    result = subprocess.run(
        [sys.executable, script],
        capture_output=False,
        timeout=300,
    )
    return result.returncode == 0


def merge_all_csvs():
    """Merge all output CSVs into master lead file."""
    all_leads = []
    
    for key, output_file in OUTPUT_FILES.items():
        if not Path(output_file).exists():
            print(f"  ⚠️  {output_file} not found — skipping")
            continue
            
        with open(output_file, newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            for row in rows:
                row["_source_scraper"] = key
                row["_source_description"] = SCRAPERS[key][1]
            all_leads.extend(rows)
            print(f"  ✅ Loaded {len(rows)} leads from {output_file}")
    
    return all_leads


def score_and_rank(leads):
    """Score leads by urgency, value, and actionability."""
    
    def lead_score(lead):
        score = 0
        
        # Priority field
        priority = str(lead.get("priority", "")).upper()
        if priority == "HIGH":
            score += 30
        elif priority == "MEDIUM":
            score += 15
            
        # MW capacity (utility projects)
        try:
            mw = float(str(lead.get("mw_capacity", lead.get("mw", "0"))).replace(",",""))
            if mw >= 100:
                score += 25
            elif mw >= 20:
                score += 15
            elif mw >= 5:
                score += 5
        except:
            pass
        
        # Source quality score
        source_scores = {
            "TRIC_KNOWN": 40,  # Direct Erick opportunity
            "SOLAR_EPC_FIRMS": 35,  # Direct callout
            "NV_MINING_SEED": 30,  # Nevada focus
            "EDGAR": 25,  # Public company = verified
            "MISO": 20,
            "ERCOT": 20,
            "EPA_FRS": 15,
            "BLM_LR2000": 15,
        }
        source = str(lead.get("source", "")).upper()
        for k, v in source_scores.items():
            if k in source:
                score += v
                break
        
        # Action contains phone number = immediately actionable
        action = str(lead.get("action", "") + lead.get("epc_contact", "")).lower()
        if any(c.isdigit() for c in action) and "-" in action:
            score += 20
        
        # State priorities
        state = str(lead.get("state", "")).upper()
        if state in ["NV", "TX"]:
            score += 10
        elif state in ["IL", "CA"]:
            score += 5
        
        return -score  # Negative for ascending sort (highest first)
    
    leads.sort(key=lead_score)
    
    # Add rank
    for i, lead in enumerate(leads):
        lead["_rank"] = i + 1
    
    return leads


def generate_summary(leads):
    """Generate actionable daily brief."""
    summary = []
    summary.append("=" * 70)
    summary.append(f"CRANEGENIUS INTEL BRIEF — {datetime.now().strftime('%B %d, %Y')}")
    summary.append("=" * 70)
    summary.append(f"\nTotal leads identified: {len(leads)}")
    
    # By source
    summary.append("\nBREAKDOWN BY SOURCE:")
    source_counts = {}
    for lead in leads:
        src = lead.get("_source_description", "Unknown")
        source_counts[src] = source_counts.get(src, 0) + 1
    for src, count in sorted(source_counts.items(), key=lambda x: -x[1]):
        summary.append(f"  {count:3d}  {src}")
    
    # Top 10 immediate actions
    summary.append("\n" + "=" * 70)
    summary.append("TOP 10 IMMEDIATE ACTIONS (call today):")
    summary.append("=" * 70)
    
    action_leads = [l for l in leads[:30] if l.get("action") or l.get("epc_contact")]
    
    for i, lead in enumerate(action_leads[:10]):
        company = lead.get("company", lead.get("project_name", "Unknown"))
        location = f"{lead.get('city', lead.get('county', ''))}, {lead.get('state', '')}".strip(", ")
        action = lead.get("action", lead.get("epc_contact", "Research required"))
        crane_use = lead.get("crane_use", lead.get("crane_relevance", "Crane work identified"))
        
        summary.append(f"\n#{i+1} {company}")
        if location.strip(","):
            summary.append(f"    📍 {location}")
        summary.append(f"    🏗️  {crane_use[:80]}")
        summary.append(f"    → {action[:100]}")
    
    # Nevada/Erick-specific section
    summary.append("\n" + "=" * 70)
    summary.append("ERICK (NEVADA) SPECIFIC OPPORTUNITIES:")
    summary.append("=" * 70)
    
    nv_leads = [l for l in leads if str(l.get("state", "")).upper() == "NV" 
                or "storey" in str(l.get("county","")).lower() 
                or "TRIC" in str(l.get("project_name",""))]
    
    for lead in nv_leads[:8]:
        company = lead.get("company", lead.get("project_name", ""))
        county = lead.get("county", "")
        crane = lead.get("crane_use", "")
        action = lead.get("action", "")
        summary.append(f"\n  🏗️  {company} ({county})")
        if crane:
            summary.append(f"     Crane work: {crane[:70]}")
        if action:
            summary.append(f"     Action: {action[:80]}")
    
    summary_text = "\n".join(summary)
    
    with open(SUMMARY_OUTPUT, "w") as f:
        f.write(summary_text)
    
    return summary_text


def run():
    print(f"\n{'='*60}")
    print("🏗️  CRANEGENIUS INTEL SUITE — MASTER RUN")
    print(f"   {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*60}")
    
    # Determine which scrapers to run
    if "--sources" in sys.argv:
        idx = sys.argv.index("--sources")
        keys_to_run = sys.argv[idx+1].split(",")
    else:
        keys_to_run = list(SCRAPERS.keys())
    
    # Run each scraper
    results = {}
    for key in keys_to_run:
        if key not in SCRAPERS:
            print(f"⚠️  Unknown source: {key}")
            continue
        script = SCRAPERS[key][0]
        if not Path(script).exists():
            print(f"⚠️  Script not found: {script}")
            continue
        results[key] = run_scraper(key, script)
    
    # Merge and rank all outputs
    print(f"\n{'─'*60}")
    print("📊 Merging and ranking all leads...")
    all_leads = merge_all_csvs()
    
    if not all_leads:
        print("⚠️  No leads found. Check individual scrapers.")
        return
    
    ranked_leads = score_and_rank(all_leads)
    
    # Write master CSV
    all_keys = set()
    for lead in ranked_leads:
        all_keys.update(lead.keys())
    
    # Ensure _rank is first
    ordered_keys = ["_rank", "_source_description"] + sorted(
        [k for k in all_keys if not k.startswith("_")]
    ) + ["_source_scraper"]
    
    with open(MASTER_OUTPUT, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=ordered_keys, extrasaction="ignore")
        writer.writeheader()
        for lead in ranked_leads:
            writer.writerow({k: lead.get(k, "") for k in ordered_keys})
    
    print(f"\n✅ Master lead file: {MASTER_OUTPUT} ({len(ranked_leads)} leads)")
    
    # Generate summary
    print("\n📋 Generating intelligence brief...")
    summary = generate_summary(ranked_leads)
    print(f"✅ Summary: {SUMMARY_OUTPUT}")
    
    # Print top of summary
    print("\n" + summary[:2000])
    
    print(f"\n{'='*60}")
    print("✅ INTEL SUITE COMPLETE")
    print(f"   Master CSV:  {MASTER_OUTPUT}")
    print(f"   Daily Brief: {SUMMARY_OUTPUT}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    run()
