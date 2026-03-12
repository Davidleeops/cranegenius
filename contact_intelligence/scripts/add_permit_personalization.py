#!/usr/bin/env python3
from __future__ import annotations
import argparse, csv, json, re
from datetime import date, datetime
from pathlib import Path

TODAY = date.today()

def norm(v): return str(v or "").strip()
def lnorm(v): return norm(v).lower()

def parse_date(v: str):
    v = norm(v)
    if not v:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return datetime.strptime(v[:19], fmt).date()
        except Exception:
            pass
    try:
        return datetime.fromisoformat(v.replace("Z", "+00:00")).date()
    except Exception:
        return None

def safe_days_ago(v):
    try:
        return int(float(norm(v)))
    except Exception:
        return None

def short_addr(v):
    a = norm(v)
    return a if len(a) <= 56 else a[:53] + "..."

def load_permits(repo: Path):
    # Prefer feed used by opportunities page; fallback to permits_imported
    sources = [
        repo / "data/static_exports/opportunity_feed_items.json",
        repo / "data/opportunities/permits_imported.json",
    ]
    rows = []
    for p in sources:
        if not p.exists():
            continue
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        if isinstance(data, list):
            rows.extend(data)
        elif isinstance(data, dict):
            if isinstance(data.get("rows"), list):
                rows.extend(data["rows"])
            elif isinstance(data.get("items"), list):
                rows.extend(data["items"])
            elif isinstance(data.get("data"), list):
                rows.extend(data["data"])
    return rows

def is_active_recent(r: dict):
    status = lnorm(r.get("status") or r.get("project_status"))
    if any(x in status for x in ("closed", "complete", "completed", "cancelled", "canceled", "finaled", "withdrawn")):
        return False

    sd = parse_date(r.get("start_date"))
    da = safe_days_ago(r.get("daysAgo"))
    issued = parse_date(r.get("issued_at") or r.get("observed_at") or r.get("effective_date"))

    ok_recent = False
    if sd:
        delta = (sd - TODAY).days
        if -90 <= delta <= 60:
            ok_recent = True
    if da is not None and 0 <= da <= 180:
        ok_recent = True
    if issued:
        age = (TODAY - issued).days
        if 0 <= age <= 180:
            ok_recent = True

    if not ok_recent:
        return False

    if not norm(r.get("permit_address")) or not norm(r.get("city")) or not norm(r.get("state")):
        return False
    return True

def token_set(v: str):
    t = re.sub(r"[^a-z0-9\s]", " ", lnorm(v))
    return {x for x in t.split() if len(x) > 2}

def score_match(contact: dict, permit: dict):
    score = 0.0
    c_city, c_state = lnorm(contact.get("city")), lnorm(contact.get("state"))
    p_city, p_state = lnorm(permit.get("city")), lnorm(permit.get("state"))

    if c_state and p_state and c_state == p_state:
        score += 3.0
    if c_city and p_city and c_city == p_city:
        score += 4.0

    c_company = token_set(contact.get("company_name", ""))
    p_text = " ".join([
        norm(permit.get("title")),
        norm(permit.get("description")),
        norm(permit.get("company")),
        norm(permit.get("permit_type")),
    ])
    p_tokens = token_set(p_text)
    overlap = len(c_company & p_tokens)
    if overlap:
        score += min(5.0, 1.5 * overlap)

    est = norm(permit.get("estimated_value"))
    # slight preference for larger/known-value records
    if est and est not in ("0", "0.0", "unknown", "n/a", ""):
        score += 0.5

    return score

def make_lines(permit: dict):
    permit_type = norm(permit.get("permit_type") or permit.get("title") or "project")
    addr = short_addr(permit.get("permit_address"))
    city = norm(permit.get("city"))
    state = norm(permit.get("state"))
    val = norm(permit.get("estimated_value"))

    line1 = f"Saw a recent {permit_type} permit signal near {addr} in {city}, {state}."
    if val:
        line2 = "If that scope is moving, we can turn around lift options quickly for that site."
    else:
        line2 = "If that scope is moving, we can map lift coverage options quickly."
    return line1, line2

def enrich_file(path: Path, permits: list[dict]):
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        cols = list(reader.fieldnames or [])

    add_cols = [
        "permit_context_id",
        "permit_recency_bucket",
        "personalization_line_1",
        "personalization_line_2",
    ]
    for c in add_cols:
        if c not in cols:
            cols.append(c)

    matched = 0
    for r in rows:
        best = None
        best_score = -1.0
        for p in permits:
            s = score_match(r, p)
            if s > best_score:
                best_score = s
                best = p

        if best is not None and best_score >= 2.5:
            line1, line2 = make_lines(best)
            r["permit_context_id"] = norm(best.get("id") or best.get("permit_id") or "")
            r["permit_recency_bucket"] = "active_recent_or_soon"
            r["personalization_line_1"] = line1
            r["personalization_line_2"] = line2
            matched += 1
        else:
            r["permit_context_id"] = ""
            r["permit_recency_bucket"] = ""
            r["personalization_line_1"] = ""
            r["personalization_line_2"] = ""

    out_path = path.with_name(path.stem + "_personalized.csv")
    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)

    return len(rows), matched, out_path

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", default=".")
    ap.add_argument("--inputs", nargs="+", required=True)
    args = ap.parse_args()

    repo = Path(args.repo).resolve()
    all_permits = load_permits(repo)
    permits = [p for p in all_permits if is_active_recent(p)]

    print(f"permits_loaded_total={len(all_permits)}")
    print(f"permits_active_recent={len(permits)}")

    for i in args.inputs:
        p = Path(i).resolve()
        total, matched, outp = enrich_file(p, permits)
        print(f"{p.name}: total={total} matched={matched} out={outp}")

if __name__ == "__main__":
    main()
