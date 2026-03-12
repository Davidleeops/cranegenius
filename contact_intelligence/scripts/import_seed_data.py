#!/usr/bin/env python3
"""
contact_intelligence/scripts/import_seed_data.py
Import a seed CSV and:
  1. Write raw rows to source_records (type=seed)
  2. Create/update canonical_companies
  3. Create company_aliases from alternate names
  4. Create domain_evidence rows for any domain found
  5. Create contact_patterns if email pattern is detectable

This is the entry point for the seed→canonicalization→evidence loop.

Usage (from repo root):
    python3 contact_intelligence/scripts/import_seed_data.py \
        --file ~/data_runtime/seeds/crane_companies_seed.csv \
        --source manual_seed_2025 \
        --mappings contact_intelligence/config/field_mappings.example.json
"""

import os, sys, csv, json, re, sqlite3, argparse
from pathlib import Path

DEFAULT_DB = os.path.expanduser("~/data_runtime/cranegenius_ci.db")

STRIP_SUFFIX = re.compile(
    r'\b(inc\.?|llc\.?|ltd\.?|corp\.?|co\.?|company|group|holdings?|'
    r'services?|solutions?|contractors?|construction|enterprises?|'
    r'associates?|international|national|global|partners?)\b',
    re.IGNORECASE
)

DEFAULT_MAP = {
    "company_name":  ["company_name","company","business_name"],
    "alias_name":    ["alias","also_known_as","dba","alternate_name"],
    "domain":        ["domain","website","company_domain"],
    "email_pattern": ["email_pattern","pattern"],
    "city":          ["city","location_city"],
    "state":         ["state","location_state"],
    "sector":        ["sector","sector_name","industry","vertical"],
    "company_type":  ["company_type","type"],
    "evidence_type": ["evidence_type","domain_source"],
    "notes":         ["notes"],
}


def norm_company(name):
    if not name: return ""
    n = STRIP_SUFFIX.sub("", name.strip().lower())
    n = re.sub(r'[^\w\s]', '', n)
    return re.sub(r'\s+', ' ', n).strip()


def detect_email_pattern(email: str, domain: str) -> str | None:
    if not email or not domain: return None
    local = email.lower().split("@")[0]
    parts = re.split(r'[.\-_]', local)
    if len(parts) == 2:
        return f"{{first}}.{{last}}@{domain}"
    elif len(parts) == 1 and len(local) > 3:
        return f"{{first}}@{domain}"
    return None


def get_sector_id(cur, sector_name):
    if not sector_name: return None
    cur.execute("SELECT sector_id FROM sectors WHERE sector_name LIKE ?", (f"%{sector_name}%",))
    r = cur.fetchone()
    return r[0] if r else None


def get_db(arg=None):
    return arg or os.environ.get("CRANEGENIUS_CI_DB", DEFAULT_DB)


def resolve(headers, mapping):
    hl = [h.lower().strip() for h in headers]
    out = {}
    for canon, candidates in mapping.items():
        for c in candidates:
            if c.lower() in hl:
                out[canon] = headers[hl.index(c.lower())]
                break
    return out


def run(file_path, source_name, mappings_path=None, db_path=None):
    db_path = get_db(db_path)
    if not Path(db_path).exists():
        sys.exit(f"[seed] DB not found: {db_path}")

    col_map = DEFAULT_MAP.copy()
    if mappings_path and Path(mappings_path).exists():
        custom = json.loads(Path(mappings_path).read_text())
        col_map.update({k: v for k, v in custom.items() if not k.startswith("_")})

    conn = sqlite3.connect(db_path)
    cur  = conn.cursor()
    imported = canonical_added = alias_added = evidence_added = pattern_added = 0

    with open(file_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        cm = resolve(reader.fieldnames or [], col_map)

        for row in reader:
            payload = json.dumps(dict(row), ensure_ascii=False)
            co_name  = row.get(cm.get("company_name",""), "").strip()
            alias    = row.get(cm.get("alias_name",""),   "").strip() or None
            domain   = row.get(cm.get("domain",""),       "").strip().lower() or None
            e_pat    = row.get(cm.get("email_pattern",""),"").strip() or None
            city     = row.get(cm.get("city",""),         "").strip() or None
            state    = row.get(cm.get("state",""),        "").strip().upper()[:2] or None
            sector   = row.get(cm.get("sector",""),       "").strip() or None
            co_type  = row.get(cm.get("company_type",""), "").strip() or None
            ev_type  = row.get(cm.get("evidence_type",""),"").strip() or "manual"

            # raw source record
            cur.execute("""INSERT INTO source_records
                (source_name,source_type,business_line,original_company_name,raw_payload)
                VALUES(?,?,?,?,?)""",
                (source_name, "seed", "cranegenius", co_name or None, payload))
            imported += 1

            if not co_name: continue
            norm = norm_company(co_name)
            sector_id = get_sector_id(cur, sector)

            # find or create canonical company
            canon_id = None
            if domain:
                cur.execute("SELECT canonical_company_id FROM canonical_companies WHERE primary_domain=?", (domain,))
                r = cur.fetchone(); canon_id = r[0] if r else None
            if not canon_id and norm:
                cur.execute("SELECT canonical_company_id FROM canonical_companies WHERE normalized_name=?", (norm,))
                r = cur.fetchone(); canon_id = r[0] if r else None

            if not canon_id:
                cur.execute("""INSERT INTO canonical_companies
                    (canonical_name,normalized_name,primary_domain,sector_id,company_type,
                     location_city,location_state,verified_status,promoted_from)
                    VALUES(?,?,?,?,?,?,?,'unverified',?)""",
                    (co_name, norm, domain, sector_id, co_type, city, state, source_name))
                canon_id = cur.lastrowid
                canonical_added += 1
            else:
                # fill blanks
                cur.execute("""UPDATE canonical_companies SET
                    primary_domain=COALESCE(primary_domain,?),
                    sector_id=COALESCE(sector_id,?),
                    company_type=COALESCE(company_type,?),
                    location_city=COALESCE(location_city,?),
                    location_state=COALESCE(location_state,?),
                    updated_at=CURRENT_TIMESTAMP
                    WHERE canonical_company_id=?""",
                    (domain, sector_id, co_type, city, state, canon_id))

            # alias
            if alias:
                norm_alias = norm_company(alias)
                cur.execute("SELECT alias_id FROM company_aliases WHERE canonical_company_id=? AND normalized_alias=?",
                            (canon_id, norm_alias))
                if not cur.fetchone():
                    cur.execute("INSERT INTO company_aliases (canonical_company_id,alias_name,normalized_alias,source) VALUES(?,?,?,?)",
                                (canon_id, alias, norm_alias, source_name))
                    alias_added += 1

            # domain evidence
            if domain:
                cur.execute("SELECT evidence_id FROM domain_evidence WHERE canonical_company_id=? AND domain_candidate=?",
                            (canon_id, domain))
                if not cur.fetchone():
                    cur.execute("""INSERT INTO domain_evidence
                        (canonical_company_id,domain_candidate,evidence_type,evidence_score,confidence_score,verified_status)
                        VALUES(?,?,?,?,?,'candidate')""",
                        (canon_id, domain, ev_type, 0.7, 0.7))
                    evidence_added += 1

            # contact pattern
            if e_pat and domain:
                cur.execute("SELECT pattern_id FROM contact_patterns WHERE canonical_company_id=? AND pattern_template=?",
                            (canon_id, e_pat))
                if not cur.fetchone():
                    cur.execute("""INSERT INTO contact_patterns
                        (canonical_company_id,domain,pattern_template,source)
                        VALUES(?,?,?,?)""",
                        (canon_id, domain, e_pat, source_name))
                    pattern_added += 1

    conn.commit(); conn.close()
    print(f"[seed] ✓ source_records: {imported}")
    print(f"[seed]   canonical_companies: +{canonical_added}")
    print(f"[seed]   company_aliases:     +{alias_added}")
    print(f"[seed]   domain_evidence:     +{evidence_added}")
    print(f"[seed]   contact_patterns:    +{pattern_added}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--file",     required=True)
    p.add_argument("--source",   required=True)
    p.add_argument("--mappings", default=None)
    p.add_argument("--db",       default=None)
    a = p.parse_args()
    run(a.file, a.source, a.mappings, a.db)
