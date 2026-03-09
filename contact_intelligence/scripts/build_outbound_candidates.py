#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from urllib.parse import urlparse

EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")

MINI_SOFT_KEYWORDS = {
    "mechanical": 2.0,
    "retrofit": 2.5,
    "rooftop": 3.0,
    "equipment install": 3.0,
    "equipment installation": 3.0,
    "tight access": 3.5,
    "tight-access": 3.5,
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

FIELD_ALIASES = {
    "company": ["company_name", "company", "account", "account_name", "employer", "organization", "org", "business_name"],
    "email": ["email", "email_address", "work_email", "primary_email"],
    "first_name": ["first_name", "firstname", "given_name"],
    "last_name": ["last_name", "lastname", "surname", "family_name"],
    "name": ["full_name", "contact_name", "name", "person_name"],
    "city": ["city", "town", "locality"],
    "state": ["state", "province", "region", "state_code", "province_code"],
    "domain": ["domain", "company_domain", "email_domain", "website_domain"],
    "website": ["website", "company_website", "url", "company_url"],
}

PROJECT_SOURCES = [
    "data/static_exports/project_candidates.json",
    "data/static_exports/top_project_candidates.json",
    "data/static_exports/recommended_expansion_candidates.json",
]

OUTPUT_COLUMNS = [
    "company_name",
    "contact_name",
    "email",
    "domain",
    "city",
    "state",
    "project_name",
    "signal_type",
    "opportunity_type",
    "targeting_segment",
    "reason_for_targeting",
    "source_file",
]

def norm(v: object) -> str:
    return str(v or "").strip()

def norm_key(v: object) -> str:
    s = norm(v).lower()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    return re.sub(r"\s+", " ", s).strip()

def tokenize(v: object) -> set[str]:
    return {t for t in norm_key(v).split() if len(t) > 2}

def extract_domain(v: str) -> str:
    s = norm(v).lower()
    if not s:
        return ""
    if "@" in s and EMAIL_RE.match(s):
        return s.split("@", 1)[1]
    if not s.startswith(("http://", "https://")):
        s = "https://" + s
    try:
        host = urlparse(s).netloc.lower()
        if host.startswith("www."):
            host = host[4:]
        return host
    except Exception:
        return ""

def first_present(row: dict, aliases: list[str]) -> str:
    for a in aliases:
        if a in row and norm(row[a]):
            return norm(row[a])
    return ""

def pick_columns(fieldnames: list[str]) -> dict[str, str]:
    idx = {f.lower().strip(): f for f in fieldnames}
    out = {}
    for key, aliases in FIELD_ALIASES.items():
        for a in aliases:
            if a in idx:
                out[key] = idx[a]
                break
    return out

def read_json_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for k in ("rows", "items", "data"):
            if isinstance(data.get(k), list):
                return data[k]
    return []

def load_projects(repo: Path, counts: Counter) -> list[dict]:
    rows = []
    seen = set()
    for rel in PROJECT_SOURCES:
        p = repo / rel
        data = read_json_rows(p)
        counts[f"projects_loaded:{rel}"] += len(data)
        for r in data:
            company = norm(r.get("company_name"))
            project = norm(r.get("project_name"))
            city = norm(r.get("city"))
            state = norm(r.get("state")).upper()
            dedupe_key = (norm_key(company), norm_key(project), norm_key(city), state)
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            x = dict(r)
            x["_source_file"] = rel
            rows.append(x)
    counts["projects_unique_after_dedupe"] = len(rows)
    return rows

def contact_csv_candidates(repo: Path) -> list[Path]:
    out = []
    for p in repo.rglob("*.csv"):
        sp = str(p).replace("\\", "/")
        if "/runs/" in sp:
            continue
        if "/data/outbound/" in sp:
            continue
        out.append(p)
    return out

def load_contacts_from_csv(path: Path, repo: Path, source_label: str, counts: Counter, source_breakdown: dict) -> list[dict]:
    contacts = []
    seen_local = set()
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                counts[f"source:{source_label}:skipped_no_header"] += 1
                return contacts
            cols = pick_columns(reader.fieldnames)
            if "email" not in cols and "name" not in cols and "first_name" not in cols:
                counts[f"source:{source_label}:skipped_not_contact_like"] += 1
                return contacts

            for raw in reader:
                row = {k.lower().strip(): norm(v) for k, v in raw.items() if k is not None}
                counts[f"source:{source_label}:rows_seen"] += 1
                source_breakdown[source_label]["rows_seen"] += 1

                email = first_present(row, FIELD_ALIASES["email"]).lower()
                if not email:
                    counts[f"source:{source_label}:excluded_blank_email"] += 1
                    source_breakdown[source_label]["excluded_blank_email"] += 1
                    continue
                if not EMAIL_RE.match(email):
                    counts[f"source:{source_label}:excluded_malformed_email"] += 1
                    source_breakdown[source_label]["excluded_malformed_email"] += 1
                    continue
                if email in seen_local:
                    counts[f"source:{source_label}:excluded_duplicate_within_source"] += 1
                    source_breakdown[source_label]["excluded_duplicate_within_source"] += 1
                    continue

                company = first_present(row, FIELD_ALIASES["company"])
                first = first_present(row, FIELD_ALIASES["first_name"])
                last = first_present(row, FIELD_ALIASES["last_name"])
                fullname = first_present(row, FIELD_ALIASES["name"]) or f"{first} {last}".strip()
                city = first_present(row, FIELD_ALIASES["city"])
                state = first_present(row, FIELD_ALIASES["state"]).upper()
                domain = extract_domain(first_present(row, FIELD_ALIASES["domain"]))
                if not domain:
                    domain = extract_domain(first_present(row, FIELD_ALIASES["website"]))
                if not domain:
                    domain = extract_domain(email)

                if not company and not domain:
                    counts[f"source:{source_label}:excluded_missing_company_and_domain"] += 1
                    source_breakdown[source_label]["excluded_missing_company_and_domain"] += 1
                    continue

                contacts.append(
                    {
                        "company_name": company,
                        "company_key": norm_key(company),
                        "company_tokens": tokenize(company),
                        "contact_name": fullname,
                        "email": email,
                        "domain": domain,
                        "city": city,
                        "state": state,
                        "source_file": str(path.relative_to(repo)) if str(path).startswith(str(repo)) else str(path),
                    }
                )
                seen_local.add(email)
                counts[f"source:{source_label}:usable"] += 1
                source_breakdown[source_label]["usable"] += 1
    except Exception as e:
        counts[f"source:{source_label}:read_error"] += 1
        source_breakdown[source_label]["read_error"] += 1
        source_breakdown[source_label]["error_message"] = str(e)
    return contacts

def load_all_contacts(repo: Path, extra_csvs: list[str], counts: Counter) -> tuple[list[dict], dict]:
    source_breakdown = defaultdict(Counter)
    contacts = []

    internal = contact_csv_candidates(repo)
    counts["internal_csv_files_scanned"] = len(internal)
    for p in internal:
        contacts.extend(load_contacts_from_csv(p, repo, f"internal:{p.name}", counts, source_breakdown))

    for x in extra_csvs:
        p = Path(x).expanduser().resolve()
        contacts.extend(load_contacts_from_csv(p, repo, f"extra:{p.name}", counts, source_breakdown))

    # global email dedupe
    unique = []
    seen = set()
    for c in contacts:
        if c["email"] in seen:
            counts["excluded_duplicate_email_global"] += 1
            continue
        seen.add(c["email"])
        unique.append(c)

    counts["contacts_total_usable_after_global_dedupe"] = len(unique)
    return unique, source_breakdown

def mini_score(project: dict) -> tuple[float, list[str]]:
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
    try:
        existing = float(project.get("mini_crane_fit_score") or 0)
    except Exception:
        existing = 0.0
    score += min(5.0, existing / 20.0)
    return round(score, 2), sorted(set(hits))

def opportunity_type(project: dict) -> str:
    v = norm(project.get("vertical")).lower()
    pt = norm(project.get("project_type")).lower()
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

def company_overlap_score(a_tokens: set[str], b_tokens: set[str]) -> float:
    if not a_tokens or not b_tokens:
        return 0.0
    inter = len(a_tokens & b_tokens)
    if inter == 0:
        return 0.0
    return inter / max(1, min(len(a_tokens), len(b_tokens)))

def build_indexes(contacts: list[dict]):
    by_company = defaultdict(list)
    by_domain = defaultdict(list)
    by_state = defaultdict(list)
    by_metro = defaultdict(list)

    for c in contacts:
        if c["company_key"]:
            by_company[c["company_key"]].append(c)
        if c["domain"]:
            by_domain[c["domain"]].append(c)
        if c["state"]:
            by_state[c["state"]].append(c)
        metro_key = (norm_key(c["city"]), c["state"])
        if metro_key[0] and metro_key[1]:
            by_metro[metro_key].append(c)

    # derive dominant company->domain map
    company_domain = {}
    for ck, arr in by_company.items():
        freq = Counter([a["domain"] for a in arr if a["domain"]])
        if freq:
            company_domain[ck] = freq.most_common(1)[0][0]
    return by_company, by_domain, by_state, by_metro, company_domain

def pick_contacts_for_project(project: dict, indexes, counts: Counter) -> tuple[list[dict], str]:
    by_company, by_domain, by_state, by_metro, company_domain = indexes
    company = norm(project.get("company_name"))
    company_key = norm_key(company)
    company_tokens = tokenize(company)
    city_key = norm_key(project.get("city"))
    state = norm(project.get("state")).upper()

    # tier 1 exact company
    if company_key and company_key in by_company:
        counts["match_tier_exact_company"] += 1
        return by_company[company_key][:8], "exact_company"

    # tier 2 soft normalized company overlap
    if company_tokens:
        soft = []
        for ck, arr in by_company.items():
            ov = company_overlap_score(company_tokens, tokenize(ck))
            if ov >= 0.5:
                soft.extend(arr[:3])
        if soft:
            # dedupe by email
            seen = set()
            dedup = []
            for c in soft:
                if c["email"] in seen:
                    continue
                seen.add(c["email"])
                dedup.append(c)
            counts["match_tier_soft_company"] += 1
            return dedup[:8], "soft_company"

    # tier 3 domain by dominant company domain
    dom = company_domain.get(company_key, "")
    if dom and dom in by_domain:
        counts["match_tier_domain"] += 1
        return by_domain[dom][:8], "domain"

    # tier 4 metro
    mk = (city_key, state)
    if city_key and state and mk in by_metro:
        counts["match_tier_metro"] += 1
        return by_metro[mk][:5], "metro"

    # tier 5 state
    if state and state in by_state:
        counts["match_tier_state"] += 1
        return by_state[state][:5], "state"

    counts["projects_without_contact_match"] += 1
    return [], "none"

def write_csv(path: Path, rows: list[dict], columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=columns)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in columns})

def main():
    ap = argparse.ArgumentParser(description="Build outbound send lists from project intelligence + contact datasets.")
    ap.add_argument("--repo", default=".")
    ap.add_argument("--max-sends", type=int, default=2000)
    ap.add_argument("--extra-csv", action="append", default=[], help="Path to external/legacy contact CSV. Repeatable.")
    args = ap.parse_args()

    repo = Path(args.repo).resolve()
    counts = Counter()

    projects = load_projects(repo, counts)
    contacts, source_breakdown = load_all_contacts(repo, args.extra_csv, counts)
    indexes = build_indexes(contacts)

    # prioritize stronger projects
    def rank_key(p):
        try:
            m = float(p.get("monetization_score") or 0)
        except Exception:
            m = 0
        try:
            c = float(p.get("confidence_score") or 0)
        except Exception:
            c = 0
        return (-m, -c)

    broad_rows = []
    mini_rows = []
    used_emails = set()

    for p in sorted(projects, key=rank_key):
        project_name = norm(p.get("project_name"))
        company_name = norm(p.get("company_name"))
        if not project_name and not company_name:
            counts["excluded_missing_project_and_company"] += 1
            continue

        matched, tier = pick_contacts_for_project(p, indexes, counts)
        if not matched:
            continue

        ms, mhits = mini_score(p)
        segment = "likely_spider_mini" if ms >= 4.5 else "broad_project"
        opp_type = opportunity_type(p)

        reason_parts = [f"match_tier:{tier}"]
        if mhits:
            reason_parts.append("mini_signals:" + ",".join(mhits[:6]))
        rr = norm(p.get("recommendation_reason"))
        pr = norm(p.get("priority_reason"))
        if rr:
            reason_parts.append(f"rec:{rr}")
        if pr:
            reason_parts.append(f"prio:{pr}")

        for c in matched:
            if c["email"] in used_emails:
                counts["excluded_duplicate_email_final"] += 1
                continue
            if not c["email"] or not EMAIL_RE.match(c["email"]):
                counts["excluded_bad_email_final"] += 1
                continue
            if not company_name and not c["domain"]:
                counts["excluded_missing_company_and_domain_final"] += 1
                continue

            row = {
                "company_name": company_name or c["company_name"],
                "contact_name": c["contact_name"],
                "email": c["email"],
                "domain": c["domain"],
                "city": norm(p.get("city")) or c["city"],
                "state": norm(p.get("state")).upper() or c["state"],
                "project_name": project_name,
                "signal_type": norm(p.get("project_type")),
                "opportunity_type": opp_type,
                "targeting_segment": segment,
                "reason_for_targeting": " | ".join(reason_parts)[:600],
                "source_file": f'{p.get("_source_file","")} + {c["source_file"]}',
            }

            broad_rows.append(row)
            if segment == "likely_spider_mini":
                mini_rows.append(row)
            used_emails.add(c["email"])
            counts["rows_output_total"] += 1
            if counts["rows_output_total"] >= args.max_sends:
                break
        if counts["rows_output_total"] >= args.max_sends:
            break

    out_dir = repo / "data" / "outbound"
    summary_json = repo / "runs" / "outbound_generation_summary.json"
    summary_md = repo / "runs" / "outbound_generation_summary.md"

    write_csv(out_dir / "outbound_likely_spider_mini.csv", mini_rows, OUTPUT_COLUMNS)
    write_csv(out_dir / "outbound_broader_projects.csv", broad_rows, OUTPUT_COLUMNS)

    exclusion_counts = {k: v for k, v in counts.items() if "excluded_" in k or "without_contact_match" in k}

    summary = {
        "files_created": [
            "data/outbound/outbound_likely_spider_mini.csv",
            "data/outbound/outbound_broader_projects.csv",
        ],
        "project_counts": {k: v for k, v in counts.items() if k.startswith("projects_")},
        "contact_counts": {k: v for k, v in counts.items() if k.startswith("source:") or k.startswith("contacts_") or k.endswith("_csv_files_scanned")},
        "matching_counts": {k: v for k, v in counts.items() if k.startswith("match_tier_")},
        "build_counts": dict(counts),
        "total_usable_outbound_rows": len(broad_rows),
        "mini_rows": len(mini_rows),
        "broad_rows": len(broad_rows),
        "top_exclusion_reasons": Counter(exclusion_counts).most_common(15),
        "source_breakdown": {k: dict(v) for k, v in source_breakdown.items()},
        "extra_csv_inputs": args.extra_csv,
    }

    summary_json.parent.mkdir(parents=True, exist_ok=True)
    summary_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    md = [
        "# Outbound Generation Summary",
        "",
        f"- Total usable outbound rows: **{len(broad_rows)}**",
        f"- Likely spider/mini rows: **{len(mini_rows)}**",
        f"- Broader opportunity rows: **{len(broad_rows)}**",
        "",
        "## Files Created",
        "- `data/outbound/outbound_likely_spider_mini.csv`",
        "- `data/outbound/outbound_broader_projects.csv`",
        "",
        "## Primary Bottlenecks",
    ]
    for k, v in summary["top_exclusion_reasons"]:
        md.append(f"- `{k}`: {v}")
    md.append("")
    md.append("## Matching Tier Counts")
    for k, v in sorted(summary["matching_counts"].items()):
        md.append(f"- `{k}`: {v}")

    summary_md.write_text("\n".join(md) + "\n", encoding="utf-8")

    print("Created:", out_dir / "outbound_likely_spider_mini.csv")
    print("Created:", out_dir / "outbound_broader_projects.csv")
    print("Summary:", summary_json)
    print("Total usable outbound rows:", len(broad_rows))

if __name__ == "__main__":
    main()
