#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, List, Tuple

EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")

MINI_SOFT_KEYWORDS = {
    "mechanical": 2.0,
    "retrofit": 2.5,
    "rooftop": 3.0,
    "equipment install": 3.0,
    "equipment installation": 3.0,
    "tight access": 3.5,
    "constrained": 2.0,
    "facade": 3.0,
    "glazing": 3.0,
    "glass": 2.0,
    "storefront": 2.5,
    "hospital": 2.0,
    "healthcare": 2.0,
    "interior": 2.5,
    "urban": 1.0,
    "commercial": 1.0,
    "window replacement": 3.0,
    "rtu replacement": 3.0,
    "chiller replacement": 3.0,
    "air handler": 2.5,
}

def norm(v: object) -> str:
    return str(v or "").strip()

def norm_key(v: object) -> str:
    s = norm(v).lower()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    return re.sub(r"\s+", " ", s).strip()

def domain_from_email(email: str) -> str:
    if "@" not in email:
        return ""
    return email.split("@", 1)[1].lower().strip()

def read_json_rows(path: Path) -> List[Dict]:
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("rows", "items", "data"):
            if isinstance(data.get(key), list):
                return data[key]
    return []

def find_contact_csvs(repo: Path) -> List[Path]:
    candidates = []
    for p in repo.rglob("*.csv"):
        sp = str(p)
        if "/runs/" in sp:
            continue
        if "outbound_" in p.name:
            continue
        candidates.append(p)
    return candidates

def load_contacts(repo: Path) -> Tuple[List[Dict], Counter]:
    counts = Counter()
    out: List[Dict] = []
    seen_email = set()

    csv_paths = find_contact_csvs(repo)
    for p in csv_paths:
        try:
            with p.open("r", encoding="utf-8-sig", newline="") as f:
                r = csv.DictReader(f)
                if not r.fieldnames:
                    continue
                fns = {x.lower(): x for x in r.fieldnames}
                email_col = next((fns[k] for k in fns if "email" == k or k.endswith("_email")), None)
                if not email_col:
                    continue

                company_col = next((fns[k] for k in fns if "company_name" in k or k == "company"), None)
                first_col = next((fns[k] for k in fns if k == "first_name"), None)
                last_col = next((fns[k] for k in fns if k == "last_name"), None)
                name_col = next((fns[k] for k in fns if k in ("full_name", "contact_name", "name")), None)
                city_col = next((fns[k] for k in fns if k == "city"), None)
                state_col = next((fns[k] for k in fns if k == "state"), None)

                for row in r:
                    counts["contact_rows_seen"] += 1
                    email = norm(row.get(email_col)).lower()
                    if not email:
                        counts["excluded_blank_email"] += 1
                        continue
                    if not EMAIL_RE.match(email):
                        counts["excluded_malformed_email"] += 1
                        continue
                    if email in seen_email:
                        counts["excluded_duplicate_email"] += 1
                        continue

                    company = norm(row.get(company_col)) if company_col else ""
                    domain = domain_from_email(email)
                    if not company and not domain:
                        counts["excluded_missing_company_and_domain"] += 1
                        continue

                    if name_col:
                        contact_name = norm(row.get(name_col))
                    else:
                        contact_name = f"{norm(row.get(first_col))} {norm(row.get(last_col))}".strip()

                    out.append(
                        {
                            "company_name": company,
                            "company_key": norm_key(company),
                            "contact_name": contact_name,
                            "email": email,
                            "domain": domain,
                            "city": norm(row.get(city_col)) if city_col else "",
                            "state": norm(row.get(state_col)).upper() if state_col else "",
                            "source_file": str(p.relative_to(repo)),
                        }
                    )
                    seen_email.add(email)
                    counts["contacts_usable"] += 1
        except Exception:
            counts["contact_files_failed"] += 1
            continue

    counts["contact_files_scanned"] = len(csv_paths)
    return out, counts

def load_projects(repo: Path) -> Tuple[List[Dict], Counter]:
    counts = Counter()
    base = repo / "data" / "static_exports"
    sources = [
        base / "project_candidates.json",
        base / "top_project_candidates.json",
        base / "recommended_expansion_candidates.json",
    ]
    items = []
    seen = set()

    for p in sources:
        rows = read_json_rows(p)
        counts[f"projects_loaded_{p.name}"] = len(rows)
        for r in rows:
            company = norm(r.get("company_name"))
            project = norm(r.get("project_name"))
            city = norm(r.get("city"))
            state = norm(r.get("state")).upper()
            key = (norm_key(company), norm_key(project), norm_key(city), state)
            if key in seen:
                continue
            seen.add(key)
            rr = dict(r)
            rr["_source_file"] = str(p.relative_to(repo))
            items.append(rr)

    counts["projects_unique"] = len(items)
    return items, counts

def mini_score_and_reasons(project: Dict) -> Tuple[float, List[str]]:
    text = " ".join(
        [
            norm(project.get("project_name")),
            norm(project.get("project_type")),
            norm(project.get("vertical")),
            norm(project.get("company_name")),
            norm(project.get("recommendation_reason")),
            norm(project.get("priority_reason")),
        ]
    ).lower()
    score = 0.0
    hits = []
    for kw, w in MINI_SOFT_KEYWORDS.items():
        if kw in text:
            score += w
            hits.append(kw)
    # leverage existing score if present
    try:
        existing = float(project.get("mini_crane_fit_score") or 0.0)
    except Exception:
        existing = 0.0
    score += min(5.0, existing / 20.0)
    return round(score, 2), hits

def opportunity_type(project: Dict) -> str:
    pt = norm(project.get("project_type")).lower()
    v = norm(project.get("vertical")).lower()
    if "energy" in v or "power" in v:
        return "energy"
    if "industrial" in v or "manufacturing" in v:
        return "industrial"
    if "logistics" in v or "warehouse" in v:
        return "logistics"
    if "data" in v:
        return "data_center"
    if "health" in v or "hospital" in v:
        return "healthcare"
    return pt or "general"

def match_contacts(project: Dict, contacts: List[Dict]) -> List[Dict]:
    company_key = norm_key(project.get("company_name"))
    state = norm(project.get("state")).upper()

    if not company_key:
        return []

    exact = [c for c in contacts if c["company_key"] and c["company_key"] == company_key]
    if exact:
        return exact

    contains = [c for c in contacts if c["company_key"] and (company_key in c["company_key"] or c["company_key"] in company_key)]
    if contains:
        return contains

    state_soft = [c for c in contacts if state and c["state"] == state and c["company_key"]]
    return state_soft[:3]

def build_rows(projects: List[Dict], contacts: List[Dict], max_total: int) -> Tuple[List[Dict], List[Dict], Counter]:
    counts = Counter()
    broad = []
    mini = []
    used_emails = set()

    # prioritize higher monetization/confidence first
    def keyfn(p):
        try:
            m = float(p.get("monetization_score") or 0)
        except Exception:
            m = 0
        try:
            c = float(p.get("confidence_score") or 0)
        except Exception:
            c = 0
        return (-m, -c)

    for p in sorted(projects, key=keyfn):
        proj_name = norm(p.get("project_name"))
        company = norm(p.get("company_name"))
        if not proj_name and not company:
            counts["excluded_missing_project_and_company"] += 1
            continue

        score, hits = mini_score_and_reasons(p)
        contacts_found = match_contacts(p, contacts)
        if not contacts_found:
            counts["projects_without_contact_match"] += 1
            continue

        segment = "likely_spider_mini" if score >= 4.5 else "broad_project"
        reason_bits = []
        if hits:
            reason_bits.append("mini_signals:" + ",".join(sorted(set(hits))[:5]))
        rr = norm(p.get("recommendation_reason"))
        pr = norm(p.get("priority_reason"))
        if rr:
            reason_bits.append(f"rec:{rr}")
        if pr:
            reason_bits.append(f"prio:{pr}")

        for c in contacts_found:
            email = c["email"]
            if email in used_emails:
                counts["excluded_duplicate_email_across_outputs"] += 1
                continue

            row = {
                "company_name": company or c["company_name"],
                "contact_name": c["contact_name"],
                "email": email,
                "domain": c["domain"],
                "city": norm(p.get("city")) or c["city"],
                "state": norm(p.get("state")).upper() or c["state"],
                "project_name": proj_name,
                "signal_type": norm(p.get("project_type")),
                "opportunity_type": opportunity_type(p),
                "targeting_segment": segment,
                "reason_for_targeting": " | ".join(reason_bits)[:500],
                "source_file": f'{p.get("_source_file","")} + {c["source_file"]}',
            }

            if not row["email"] or not EMAIL_RE.match(row["email"]):
                counts["excluded_bad_email_final"] += 1
                continue
            if not row["company_name"] and not row["domain"]:
                counts["excluded_missing_company_and_domain_final"] += 1
                continue

            if segment == "likely_spider_mini":
                mini.append(row)
            broad.append(row)
            used_emails.add(email)
            counts["rows_output_total"] += 1
            if counts["rows_output_total"] >= max_total:
                return broad, mini, counts

    return broad, mini, counts

def write_csv(path: Path, rows: List[Dict], columns: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=columns)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in columns})

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", default=".")
    ap.add_argument("--max-sends", type=int, default=2000)
    args = ap.parse_args()

    repo = Path(args.repo).resolve()
    out_dir = repo / "data" / "outbound"
    run_dir = repo / "runs"
    columns = [
        "company_name","contact_name","email","domain","city","state","project_name","signal_type",
        "opportunity_type","targeting_segment","reason_for_targeting","source_file"
    ]

    contacts, c_counts = load_contacts(repo)
    projects, p_counts = load_projects(repo)
    broad, mini, b_counts = build_rows(projects, contacts, max_total=args.max_sends)

    mini_path = out_dir / "outbound_likely_spider_mini.csv"
    broad_path = out_dir / "outbound_broader_projects.csv"
    write_csv(mini_path, mini, columns)
    write_csv(broad_path, broad, columns)

    summary = {
        "files_created": [str(mini_path.relative_to(repo)), str(broad_path.relative_to(repo))],
        "project_counts": dict(p_counts),
        "contact_counts": dict(c_counts),
        "build_counts": dict(b_counts),
        "total_usable_outbound_rows": len(broad),
        "mini_rows": len(mini),
        "broad_rows": len(broad),
        "top_exclusion_reasons": Counter({k:v for k,v in (c_counts + b_counts).items() if k.startswith("excluded_") or "without_contact_match" in k}).most_common(10),
    }

    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "outbound_generation_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    md = [
        "# Outbound Generation Summary",
        "",
        f"- Total usable outbound rows: **{len(broad)}**",
        f"- Likely spider/mini rows: **{len(mini)}**",
        f"- Broader project rows: **{len(broad)}**",
        "",
        "## Files Created",
        f"- `{mini_path.relative_to(repo)}`",
        f"- `{broad_path.relative_to(repo)}`",
        f"- `runs/outbound_generation_summary.json`",
        "",
        "## Top Exclusions",
    ]
    for k, v in summary["top_exclusion_reasons"]:
        md.append(f"- `{k}`: {v}")
    (run_dir / "outbound_generation_summary.md").write_text("\n".join(md) + "\n", encoding="utf-8")

    print("Created:", mini_path)
    print("Created:", broad_path)
    print("Summary:", run_dir / "outbound_generation_summary.json")
    print("Total usable outbound rows:", len(broad))

if __name__ == "__main__":
    main()
