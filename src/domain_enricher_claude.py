from __future__ import annotations
import json, logging, os, re, time
from typing import Dict, List
import pandas as pd, requests

log = logging.getLogger("cranegenius.domain_enricher")
ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
BATCH_SIZE = 20
MODEL = "claude-haiku-4-5"
_STRIP_WORDS = {"llc","inc","corp","co","ltd","lp","llp","dba","construction","constructors","contracting","contractors","group","services","solutions","company","enterprises","and","the","of","&"}

def _name_to_domain_candidates(name: str, city: str = "") -> List[str]:
    clean = name.lower()
    clean = re.sub(r"[^\w\s]", "", clean)
    words = [w for w in clean.split() if w and w not in _STRIP_WORDS]
    if not words:
        words = clean.split()[:2]
    slug = "".join(words[:4])
    slug2 = "-".join(words[:3])
    slug3 = "".join(words[:2])
    city_slug = city.lower().replace(" ", "") if city else ""
    candidates = []
    for s in [slug, slug2, slug3]:
        if len(s) < 3:
            continue
        candidates.append(f"{s}.com")
        if city_slug and city_slug not in s:
            candidates.append(f"{s}{city_slug}.com")
    return list(dict.fromkeys(candidates))

def enrich_domains_with_claude(enriched_df: pd.DataFrame) -> pd.DataFrame:
    api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    out = enriched_df.copy()
    unresolved_mask = out["contractor_domain"].isna() | (out["contractor_domain"] == "")
    unresolved = out[unresolved_mask].copy()
    if unresolved.empty:
        log.info("All domains already resolved — skipping enrichment")
        return out
    unique_names = (
        unresolved[["contractor_name_normalized", "project_city", "project_state"]]
        .drop_duplicates(subset=["contractor_name_normalized"])
        .head(100)
    )
    log.info("Domain enrichment: resolving %d unresolved contractors", len(unique_names))
    domain_lookup: Dict[str, str] = {}
    if api_key:
        rows_list = unique_names.to_dict("records")
        for i in range(0, len(rows_list), BATCH_SIZE):
            batch = rows_list[i:i + BATCH_SIZE]
            results = _query_claude_batch(batch, api_key)
            domain_lookup.update(results)
            if i + BATCH_SIZE < len(rows_list):
                time.sleep(0.5)
    else:
        log.warning("ANTHROPIC_API_KEY not set — using name-based domain generation only")
    generated_count = 0
    for _, row in unique_names.iterrows():
        name = str(row.get("contractor_name_normalized", "")).lower().strip()
        if domain_lookup.get(name):
            continue
        city = str(row.get("project_city", ""))
        candidates = _name_to_domain_candidates(name, city)
        if candidates:
            domain_lookup[name] = "|".join(candidates[:3])
            generated_count += 1
            domain_lookup[name + "__source"] = "enrichment_generated"
    log.info("Name-based generation: %d additional contractor domain sets", generated_count)
    resolved_count = 0
    for idx, row in out.iterrows():
        if out.at[idx, "contractor_domain"]:
            continue
        name = str(row.get("contractor_name_normalized", "")).lower().strip()
        if name in domain_lookup and domain_lookup[name]:
            out.at[idx, "contractor_domain"] = domain_lookup[name]
            src_tag = domain_lookup.get(name + "__source", "enrichment_confident")
            out.at[idx, "domain_resolution_source"] = src_tag
            resolved_count += 1
    log.info("Total enrichment resolved %d contractor domain entries", resolved_count)
    return out

def _query_claude_batch(batch: List[dict], api_key: str) -> Dict[str, str]:
    companies_text = "\n".join(
        f"{i+1}. {r['contractor_name_normalized']} ({r.get('project_city','')}, {r.get('project_state','')})"
        for i, r in enumerate(batch)
    )
    prompt = f"""You are a business research assistant. For each contractor below, return their website domain ONLY if you are highly confident it exists.\n\nRules:\n- Return ONLY a JSON object mapping the exact company name (as given) to its domain\n- Domain format: just the domain, no https:// or www\n- If not confident, use null\n- These are construction/electrical/mechanical contractors in Texas\n\nCompanies:\n{companies_text}\n\nReturn JSON only."""
    try:
        resp = requests.post(ANTHROPIC_API_URL, headers={"x-api-key": api_key, "anthropic-version": "2023-06-01", "content-type": "application/json"}, json={"model": MODEL, "max_tokens": 1024, "messages": [{"role": "user", "content": prompt}]}, timeout=30)
        resp.raise_for_status()
        text = resp.json()["content"][0]["text"].strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1].rsplit("```", 1)[0].strip()
        result_raw = json.loads(text)
        result: Dict[str, str] = {}
        for k, v in result_raw.items():
            norm_key = k.lower().strip()
            if v and isinstance(v, str) and "." in v:
                result[norm_key] = v.lower().strip().lstrip("www.").rstrip("/")
            else:
                result[norm_key] = ""
        log.info("  Claude batch (%d companies): %d confident domains", len(batch), sum(1 for v in result.values() if v))
        return result
    except Exception as exc:
        log.warning("Claude API batch failed: %s", exc)
        return {}
