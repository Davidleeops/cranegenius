#!/usr/bin/env python3
from __future__ import annotations
import argparse, csv, random, re
from pathlib import Path

GENERIC_PREFIXES = ("info@", "sales@", "contact@", "admin@")

PATTERN_INTERRUPTS = [
    "Hey this might be a bit out of the blue.",
    "This just crossed my mind.",
    "This might sound random.",
    "Quick thing I wanted to ask.",
    "Bit of an odd question.",
    "Something I wanted to check quickly.",
    "This may be out of left field.",
    "Not sure if you deal with this but.",
    "Something I was asked this morning.",
    "This just came up and I figured I would ask.",
]

SPIDER_QUESTIONS = [
    "Do you ever run URW295 spider cranes for tight access lifts?",
    "Do you ever deal with spider cranes on tight access jobs?",
    "Do you ever bring in spider cranes for interior lifts?",
    "Does your team ever run spider cranes for tight access work?",
    "Do you ever end up needing spider cranes on glass or atrium installs?",
]

BROAD_QUESTIONS = [
    "Do you ever handle jobs where lift access gets tight at the last minute?",
    "Does your team run into projects that need a specialty lift quickly?",
    "Do you ever need help sourcing lift equipment when access is limited?",
    "Does this come up for your team on active jobs?",
    "Do you handle jobs where standard setups will not fit?",
]

SPIDER_CONTEXT = [
    "A project manager asked if we could help locate one for a job coming up.",
    "I was asked to check who can take that kind of lift this week.",
    "This came up on an active scope and I needed a quick answer.",
]

BROAD_CONTEXT = [
    "I got asked about this on an active job and needed a quick answer.",
    "This came up on a running project where access became a problem.",
    "A PM asked me to check this before they send the work elsewhere.",
]

def n(v): return str(v or "").strip()

def title_case_if_all_caps(v: str) -> str:
    s = n(v)
    return s.title() if s.isupper() else s

def first_name(contact_name: str) -> str:
    parts = n(contact_name).split()
    return parts[0] if parts else ""

def is_spider(row: dict) -> bool:
    t = " ".join([
        n(row.get("campaign_type")),
        n(row.get("targeting_segment")),
        n(row.get("signal_type")),
    ]).lower()
    return "spider" in t or "mini" in t or "urw295" in t

def build_opening(row: dict, rng: random.Random) -> tuple[str, str]:
    pi = rng.choice(PATTERN_INTERRUPTS)
    spider = is_spider(row)
    question = rng.choice(SPIDER_QUESTIONS if spider else BROAD_QUESTIONS)
    context = rng.choice(SPIDER_CONTEXT if spider else BROAD_CONTEXT)

    city = title_case_if_all_caps(row.get("city"))
    state = title_case_if_all_caps(row.get("state"))
    loc = ", ".join([x for x in [city, state] if n(x)])

    if loc:
        context = context.replace("active job", f"active job in {loc}", 1)

    fn = first_name(row.get("contact_name"))
    if fn:
        opening = f"Hi {fn},\n\n{pi}\n\n{question}\n\n{context}\n\nWanted to check with you before I point them somewhere else.\n\nDavid"
    else:
        opening = f"{pi}\n\n{question}\n\n{context}\n\nWanted to check with you before I point them somewhere else.\n\nDavid"

    return pi, opening

def process_file(path: Path, seed: int) -> tuple[int, int, int, Path]:
    rows = list(csv.DictReader(path.open("r", encoding="utf-8-sig", newline="")))
    if not rows:
        out_path = path.with_name(path.stem + "_human.csv")
        out_path.write_text("", encoding="utf-8")
        return 0, 0, 0, out_path

    fieldnames = list(rows[0].keys())
    for c in ("first_name", "last_name", "pattern_interrupt", "email_opening_final"):
        if c not in fieldnames:
            fieldnames.append(c)

    kept = []
    removed_generic = 0
    for idx, row in enumerate(rows):
        email = n(row.get("email")).lower()
        if email.startswith(GENERIC_PREFIXES):
            removed_generic += 1
            continue

        full = n(row.get("contact_name"))
        parts = full.split()
        row["first_name"] = parts[0] if parts else ""
        row["last_name"] = " ".join(parts[1:]) if len(parts) > 1 else ""

        rng = random.Random(seed + idx)
        pi, opening = build_opening(row, rng)
        row["pattern_interrupt"] = pi
        row["email_opening_final"] = opening
        kept.append(row)

    out_path = path.with_name(path.stem + "_human.csv")
    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(kept)

    return len(rows), len(kept), removed_generic, out_path

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--inputs", nargs="+", required=True)
    ap.add_argument("--seed", type=int, default=20260310)
    args = ap.parse_args()

    for p in args.inputs:
        path = Path(p).resolve()
        total, kept, removed, out_path = process_file(path, args.seed)
        print(f"{path.name}: in={total} kept={kept} removed_generic={removed} out={out_path}")

if __name__ == "__main__":
    main()
