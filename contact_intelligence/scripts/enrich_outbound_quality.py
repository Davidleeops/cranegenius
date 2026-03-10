#!/usr/bin/env python3
from __future__ import annotations
import argparse, csv, json, re
from pathlib import Path
from collections import Counter

EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")
FREE_DOMAINS = {"gmail.com","yahoo.com","hotmail.com","outlook.com","icloud.com","aol.com","proton.me","protonmail.com","msn.com","live.com"}

INPUTS = [
    "data/outbound/legacy_broad_equipment.csv",
    "data/outbound/legacy_spider_direct.csv",
    "data/outbound/outbound_broader_projects.csv",
    "data/outbound/outbound_likely_spider_mini.csv",
]

def norm(v): return str(v or "").strip()
def to_float(v, d=0.0):
    try: return float(v)
    except Exception: return d

def domain_from_email(e):
    e = norm(e).lower()
    return e.split("@",1)[1] if "@" in e else ""

def parse_verification(row, mv_cache):
    email = norm(row.get("email")).lower()
    if email and email in mv_cache:
        r = norm(mv_cache[email].get("result")).lower()
        if r in {"valid","invalid","catchall","unknown","risky"}:
            return r
    for k in ("verification_status","email_verification_result","mv_result","millionverifier_result"):
        r = norm(row.get(k)).lower()
        if r in {"valid","invalid","catchall","unknown","risky"}:
            return r
    return "unknown"

def domain_conf(domain):
    if not domain: return 0.0
    if domain in FREE_DOMAINS: return 0.35
    if "." in domain: return 0.82
    return 0.55

def title_rel(text):
    t = norm(text).lower()
    pri = ["owner","president","operations manager","branch manager","fleet manager","equipment manager","project manager","estimator","superintendent","dispatch"]
    if any(x in t for x in pri): return 0.88
    if any(x in t for x in ["manager","director","vp"]): return 0.75
    return 0.55

def buyer_fit(row):
    t = " ".join([norm(row.get("inferred_segment")), norm(row.get("targeting_segment")), norm(row.get("reason_for_targeting")), norm(row.get("opportunity_type"))]).lower()
    score = 0.45
    if any(x in t for x in ["spider","mini","glazing","facade","rigging","crane","lifting","hvac","mechanical"]): score += 0.25
    if any(x in t for x in ["industrial","project","equipment"]): score += 0.15
    return min(0.98, round(score, 3))

def email_conf(email, domain, verification):
    s = 0.0
    if EMAIL_RE.match(email): s += 0.55
    if domain and "." in domain: s += 0.15
    if domain in FREE_DOMAINS: s -= 0.15
    if verification == "valid": s += 0.25
    if verification == "invalid": s -= 0.35
    if verification == "catchall": s -= 0.10
    return max(0.0, min(0.99, round(s, 3)))

def source_count(v):
    s = norm(v)
    return 1 + s.count(" + ") if s else 1

def read_csv(path):
    if not path.exists(): return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))

def write_csv(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    keys = set()
    for r in rows: keys.update(r.keys())
    cols = [
        "company_name","contact_name","email","domain","city","state","province","country",
        "project_name","signal_type","opportunity_type","targeting_segment","inferred_segment",
        "reason_for_targeting","source_file","source_row_number",
        "domain_confidence","title_relevance_score","buyer_fit_score","email_confidence",
        "verification_status","fallback_flag","provenance_source_count","quality_source_file"
    ] + sorted([k for k in keys if k not in {
        "company_name","contact_name","email","domain","city","state","province","country",
        "project_name","signal_type","opportunity_type","targeting_segment","inferred_segment",
        "reason_for_targeting","source_file","source_row_number",
        "domain_confidence","title_relevance_score","buyer_fit_score","email_confidence",
        "verification_status","fallback_flag","provenance_source_count","quality_source_file"
    }])
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols); w.writeheader(); w.writerows(rows)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", default=".")
    args = ap.parse_args()
    repo = Path(args.repo).resolve()

    mv_cache = {}
    mv_path = repo / "runs" / "mv_verification_cache.json"
    if mv_path.exists():
        try:
            mv_cache = json.loads(mv_path.read_text(encoding="utf-8"))
        except Exception:
            mv_cache = {}

    counts = Counter()
    out_paths = []
    for rel in INPUTS:
        rows = read_csv(repo / rel)
        counts[f"rows_seen:{rel}"] = len(rows)
        enriched = []
        for r in rows:
            email = norm(r.get("email")).lower()
            if not email: counts["excluded_blank_email"] += 1; continue
            if not EMAIL_RE.match(email): counts["excluded_malformed_email"] += 1; continue
            domain = norm(r.get("domain")).lower() or domain_from_email(email)
            company = norm(r.get("company_name"))
            if not company and not domain: counts["excluded_missing_company_and_domain"] += 1; continue

            verification = parse_verification(r, mv_cache)
            dconf = domain_conf(domain)
            tref = title_rel(" ".join([norm(r.get("contact_name")), norm(r.get("reason_for_targeting"))]))
            bfit = buyer_fit(r)
            econf = email_conf(email, domain, verification)
            fallback = 1 if (verification != "valid" and econf < 0.80) else 0

            rr = dict(r)
            rr["domain"] = domain
            rr["domain_confidence"] = dconf
            rr["title_relevance_score"] = tref
            rr["buyer_fit_score"] = bfit
            rr["email_confidence"] = econf
            rr["verification_status"] = verification
            rr["fallback_flag"] = fallback
            rr["provenance_source_count"] = source_count(r.get("source_file"))
            rr["quality_source_file"] = rel
            enriched.append(rr)
            counts["rows_enriched"] += 1

        out_rel = rel.replace("data/outbound/", "data/outbound/enriched_")
        write_csv(repo / out_rel, enriched)
        out_paths.append(out_rel)

    (repo / "runs" / "phase2_quality_metrics.json").write_text(json.dumps({"files_created": out_paths, "counts": dict(counts)}, indent=2), encoding="utf-8")
    print("Wrote runs/phase2_quality_metrics.json")
    for p in out_paths: print("Created:", p)

if __name__ == "__main__":
    main()
