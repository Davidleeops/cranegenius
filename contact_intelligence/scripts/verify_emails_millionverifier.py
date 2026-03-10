#!/usr/bin/env python3
from __future__ import annotations
import argparse, csv, json, os, re, time, urllib.parse, urllib.request
from pathlib import Path

EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")
INPUTS = [
    "data/outbound/legacy_broad_equipment.csv",
    "data/outbound/legacy_spider_direct.csv",
    "data/outbound/outbound_broader_projects.csv",
    "data/outbound/outbound_likely_spider_mini.csv",
]

def norm(v): return str(v or "").strip()

def pull_emails(repo: Path):
    out = set()
    for rel in INPUTS:
        p = repo / rel
        if not p.exists():
            continue
        with p.open("r", encoding="utf-8-sig", newline="") as f:
            for r in csv.DictReader(f):
                e = norm(r.get("email")).lower()
                if EMAIL_RE.match(e):
                    out.add(e)
    return sorted(out)

def mv_check(api_key: str, email: str):
    q = urllib.parse.urlencode({"api": api_key, "email": email, "timeout": 10})
    url = f"https://app.millionverifier.com/api/v3/?{q}"
    with urllib.request.urlopen(url, timeout=20) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    # typical field name is "result"
    result = norm(data.get("result")).lower() or "unknown"
    return {"result": result, "raw": data}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", default=".")
    ap.add_argument("--limit", type=int, default=3000)
    ap.add_argument("--sleep-ms", type=int, default=120)
    args = ap.parse_args()

    api_key = os.getenv("MILLIONVERIFIER_API_KEY", "").strip()
    if not api_key:
        raise SystemExit("Missing MILLIONVERIFIER_API_KEY")

    repo = Path(args.repo).resolve()
    emails = pull_emails(repo)[:args.limit]

    cache_path = repo / "runs" / "mv_verification_cache.json"
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache = {}
    if cache_path.exists():
        try:
            cache = json.loads(cache_path.read_text(encoding="utf-8"))
        except Exception:
            cache = {}

    checked = 0
    for e in emails:
        if e in cache and cache[e].get("result") in {"valid", "invalid", "catchall", "unknown", "risky"}:
            continue
        try:
            cache[e] = mv_check(api_key, e)
        except Exception as ex:
            cache[e] = {"result": "unknown", "error": str(ex)}
        checked += 1
        time.sleep(max(0, args.sleep_ms) / 1000.0)

    cache_path.write_text(json.dumps(cache, indent=2), encoding="utf-8")
    print("emails_total:", len(emails))
    print("emails_checked_now:", checked)
    print("cache_file:", cache_path)

if __name__ == "__main__":
    main()
