#!/usr/bin/env python3
from __future__ import annotations
import argparse, csv, json, re
from collections import Counter, defaultdict
from pathlib import Path
from urllib.parse import urlparse

EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")
BAD_DOMAIN_PATTERNS = ("mailinator.", "tempmail", "10minutemail", "guerrillamail", "trashmail", "example.com")
SPIDER_HINTS = ["glazing","glass","storefront","facade","mechanical","hvac","rigging","crane","lifting","access","industrial contractor","urban contractor","specialty contractor"]

ALIASES = {
    "company_name": ["company_name","company","account","employer","organization","business_name"],
    "email": ["email","email_address","work_email","primary_email"],
    "first_name": ["first_name","firstname","given_name"],
    "last_name": ["last_name","lastname","surname"],
    "contact_name": ["contact_name","full_name","name","person_name"],
    "city": ["city","town","locality"],
    "state": ["state","state_code","region"],
    "province": ["province","province_code"],
    "country": ["country","country_code"],
    "domain": ["domain","company_domain","email_domain","website_domain"],
    "website": ["website","company_website","url","company_url"],
    "title": ["title","job_title","role","position","department"],
    "notes": ["notes","description","industry","label"],
}

def norm(v): return str(v or "").strip()
def nkey(v): return re.sub(r"\s+"," ",re.sub(r"[^a-z0-9\s]"," ",norm(v).lower())).strip()

def pick(row, keys):
    for k in keys:
        if k and k in row and norm(row[k]): return norm(row[k])
    return ""

def domain_of(v):
    s = norm(v).lower()
    if not s: return ""
    if "@" in s and EMAIL_RE.match(s): return s.split("@",1)[1]
    if not s.startswith(("http://","https://")): s = "https://" + s
    try:
        return urlparse(s).netloc.lower().removeprefix("www.")
    except Exception:
        return ""

def map_headers(fieldnames):
    low = {f.lower().strip(): f for f in fieldnames}
    out = {}
    for k, opts in ALIASES.items():
        for o in opts:
            if o in low:
                out[k] = low[o]
                break
    return out

def segment_score(text):
    t = nkey(text)
    hits = [h for h in SPIDER_HINTS if h in t]
    return ("legacy_spider_direct" if len(hits) >= 2 else "legacy_broad_equipment"), float(len(hits)), hits

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", default=".")
    ap.add_argument("--extra-csv", action="append", default=[], required=True)
    ap.add_argument("--max-sends", type=int, default=2000)
    a = ap.parse_args()

    repo = Path(a.repo).resolve()
    out_dir = repo / "data" / "outbound"
    out_dir.mkdir(parents=True, exist_ok=True)

    counts = Counter()
    by_source = defaultdict(Counter)
    by_segment = Counter()
    seen_email = set()
    spider_rows, broad_rows = [], []

    for src in a.extra_csv:
        p = Path(src).expanduser().resolve()
        if not p.exists():
            counts["missing_source_file"] += 1
            by_source[str(p)]["missing_source_file"] += 1
            continue

        with p.open("r", encoding="utf-8-sig", newline="") as f:
            r = csv.DictReader(f)
            if not r.fieldnames:
                counts["bad_source_no_header"] += 1
                by_source[str(p)]["bad_source_no_header"] += 1
                continue

            cols = map_headers(r.fieldnames)
            row_num = 1
            for raw in r:
                row_num += 1
                row = {k.lower().strip(): norm(v) for k,v in raw.items() if k is not None}
                counts["rows_seen"] += 1
                by_source[str(p)]["rows_seen"] += 1

                email = pick(row, [cols.get("email","")]).lower()
                if not email:
                    counts["excluded_blank_email"] += 1; by_source[str(p)]["excluded_blank_email"] += 1; continue
                if not EMAIL_RE.match(email):
                    counts["excluded_malformed_email"] += 1; by_source[str(p)]["excluded_malformed_email"] += 1; continue
                if email in seen_email:
                    counts["excluded_duplicate_email"] += 1; by_source[str(p)]["excluded_duplicate_email"] += 1; continue

                domain = domain_of(pick(row,[cols.get("domain","")]))
                if not domain: domain = domain_of(pick(row,[cols.get("website","")]))
                if not domain: domain = domain_of(email)
                if any(b in domain for b in BAD_DOMAIN_PATTERNS):
                    counts["excluded_disposable_or_bad_domain"] += 1; by_source[str(p)]["excluded_disposable_or_bad_domain"] += 1; continue

                company = pick(row,[cols.get("company_name","")])
                first = pick(row,[cols.get("first_name","")])
                last = pick(row,[cols.get("last_name","")])
                cname = pick(row,[cols.get("contact_name","")]) or f"{first} {last}".strip()
                city = pick(row,[cols.get("city","")])
                state = pick(row,[cols.get("state","")])
                province = pick(row,[cols.get("province","")])
                country = pick(row,[cols.get("country","")])
                title = pick(row,[cols.get("title","")])
                notes = pick(row,[cols.get("notes","")])

                seg, score, hits = segment_score(" ".join([company, title, notes, domain, city, state, province, country]))
                reason = f"legacy_first; score={score:.1f}; hits={','.join(hits[:6])}" if hits else f"legacy_first; score={score:.1f}"

                out = {
                    "company_name": company,
                    "contact_name": cname,
                    "email": email,
                    "domain": domain,
                    "city": city,
                    "state": state,
                    "province": province,
                    "country": country,
                    "source_file": str(p),
                    "source_row_number": row_num,
                    "inferred_segment": seg,
                    "reason_for_targeting": reason,
                }

                if seg == "legacy_spider_direct":
                    spider_rows.append(out)
                broad_rows.append(out)
                by_segment[seg] += 1
                seen_email.add(email)
                counts["rows_usable"] += 1
                by_source[str(p)]["rows_usable"] += 1

                if len(broad_rows) >= a.max_sends:
                    break
            if len(broad_rows) >= a.max_sends:
                break

    cols = ["company_name","contact_name","email","domain","city","state","province","country","source_file","source_row_number","inferred_segment","reason_for_targeting"]
    spider_path = out_dir / "legacy_spider_direct.csv"
    broad_path = out_dir / "legacy_broad_equipment.csv"

    for path, rows in [(spider_path, spider_rows), (broad_path, broad_rows)]:
        with path.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            w.writerows(rows)

    summary = {
        "files_created": [str(spider_path.relative_to(repo)), str(broad_path.relative_to(repo))],
        "final_usable_rows": len(broad_rows),
        "spider_rows": len(spider_rows),
        "segment_counts": dict(by_segment),
        "exclusion_counts": {k:v for k,v in counts.items() if k.startswith("excluded_")},
        "source_file_counts": {k: dict(v) for k,v in by_source.items()},
        "all_counts": dict(counts),
    }

    runs = repo / "runs"
    runs.mkdir(exist_ok=True)
    (runs / "legacy_outbound_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    (runs / "legacy_outbound_summary.md").write_text(
        "# Legacy Outbound Summary\n\n"
        + f"- Final usable rows: **{len(broad_rows)}**\n"
        + f"- Spider direct rows: **{len(spider_rows)}**\n",
        encoding="utf-8",
    )

    print("Created:", spider_path)
    print("Created:", broad_path)
    print("Summary:", runs / "legacy_outbound_summary.json")
    print("Final usable rows:", len(broad_rows))

if __name__ == "__main__":
    main()
