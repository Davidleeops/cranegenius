#!/usr/bin/env python3
from __future__ import annotations
import argparse, csv, json, re
from pathlib import Path
from collections import Counter

EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")
INPUTS = [
    ("data/outbound/enriched_legacy_spider_direct.csv", "spider_direct"),
    ("data/outbound/enriched_outbound_likely_spider_mini.csv", "spider_direct"),
    ("data/outbound/enriched_legacy_broad_equipment.csv", "broad_equipment"),
    ("data/outbound/enriched_outbound_broader_projects.csv", "broad_equipment"),
]

REQ = [
    "company_name","contact_name","email","domain","city","state","project_name","signal_type",
    "opportunity_type","targeting_segment","reason_for_targeting","campaign_type",
    "buyer_fit_score","email_confidence","verification_status","source_file"
]

def norm(v): return str(v or "").strip()
def to_float(v, d=0.0):
    try: return float(v)
    except Exception: return d

def read_csv(path):
    if not path.exists(): return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))

def score(r):
    return (
        (1.0 if norm(r.get("verification_status")).lower()=="valid" else 0.0) * 0.45
        + to_float(r.get("email_confidence"),0.0)*0.35
        + to_float(r.get("buyer_fit_score"),0.0)*0.20
    )

def write_csv(path, rows, cols):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols); w.writeheader(); w.writerows(rows)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", default=".")
    ap.add_argument("--max-total", type=int, default=1000)
    args = ap.parse_args()
    repo = Path(args.repo).resolve()

    counts = Counter()
    spider, broad = [], []
    for rel, ctype in INPUTS:
        rows = read_csv(repo / rel)
        counts[f"rows_seen:{rel}"] = len(rows)
        for r in rows:
            email = norm(r.get("email")).lower()
            if not email or not EMAIL_RE.match(email): counts["excluded_bad_email"] += 1; continue
            rr = dict(r)
            rr["campaign_type"] = ctype
            rr["targeting_segment"] = norm(rr.get("targeting_segment")) or norm(rr.get("inferred_segment")) or ctype
            rr["buyer_fit_score"] = to_float(rr.get("buyer_fit_score"), 0.45)
            rr["email_confidence"] = to_float(rr.get("email_confidence"), 0.55)
            rr["verification_status"] = norm(rr.get("verification_status")).lower() or "unknown"
            rr["score"] = score(rr)
            if ctype == "spider_direct": spider.append(rr)
            else: broad.append(rr)

    spider.sort(key=lambda r: r["score"], reverse=True)
    broad.sort(key=lambda r: r["score"], reverse=True)

    seen = set()
    spider_out, broad_out = [], []
    spider_target = args.max_total // 2
    broad_target = args.max_total - spider_target

    for pool, out, lim in [(spider, spider_out, spider_target), (broad, broad_out, broad_target)]:
        for r in pool:
            if len(out) >= lim: break
            e = r["email"]
            if e in seen: counts["excluded_duplicate_email"] += 1; continue
            seen.add(e); out.append(r)

    # fill short side
    master = spider_out + broad_out
    if len(master) < args.max_total:
        for r in spider + broad:
            if len(master) >= args.max_total: break
            e = r["email"]
            if e in seen: continue
            seen.add(e)
            master.append(r)

    # normalize rows
    def shrink(rows):
        out = []
        for r in rows:
            x = {k: norm(r.get(k)) for k in REQ}
            out.append(x)
        return out

    spider_final = shrink([r for r in master if r.get("campaign_type")=="spider_direct"])
    broad_final = shrink([r for r in master if r.get("campaign_type")=="broad_equipment"])
    master_final = shrink(master)

    out_dir = repo / "data" / "outbound"
    write_csv(out_dir / "tomorrow_1000_master.csv", master_final, REQ)
    write_csv(out_dir / "tomorrow_1000_spider.csv", spider_final, REQ)
    write_csv(out_dir / "tomorrow_1000_broad.csv", broad_final, REQ)

    summary = {
        "total": len(master_final),
        "spider": len(spider_final),
        "broad": len(broad_final),
        "counts": dict(counts),
    }
    runs = repo / "runs"; runs.mkdir(exist_ok=True)
    (runs / "tomorrow_1000_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    (runs / "tomorrow_1000_summary.md").write_text(
        f"# Tomorrow 1000 Summary\n\n- total: **{len(master_final)}**\n- spider: **{len(spider_final)}**\n- broad: **{len(broad_final)}**\n",
        encoding="utf-8"
    )
    print("Created:", out_dir / "tomorrow_1000_master.csv")
    print("Created:", out_dir / "tomorrow_1000_spider.csv")
    print("Created:", out_dir / "tomorrow_1000_broad.csv")
    print("Wrote runs/tomorrow_1000_summary.json")

if __name__ == "__main__":
    main()
