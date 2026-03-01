from __future__ import annotations

import json
import logging
import os
import time
from typing import Dict, List

import pandas as pd
import requests

log = logging.getLogger("cranegenius.domain_enricher")

ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
BATCH_SIZE = 20
MODEL = "claude-haiku-4-5-20251001"


def enrich_domains_with_claude(enriched_df: pd.DataFrame) -> pd.DataFrame:
    api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        log.warning("ANTHROPIC_API_KEY not set — skipping Claude domain enrichment")
        return enriched_df

    out = enriched_df.copy()
    unresolved_mask = out["contractor_domain"].isna() | (out["contractor_domain"] == "")
    unresolved = out[unresolved_mask].copy()

    if unresolved.empty:
        log.info("All domains already resolved — skipping Claude enrichment")
        return out

    unique_names = (
        unresolved[["contractor_name_normalized", "project_city", "project_state"]]
        .drop_duplicates(subset=["contractor_name_normalized"])
        .head(100)
    )

    log.info("Claude domain enrichment: resolving %d unresolved contractors", len(unique_names))

    domain_lookup: Dict[str, str] = {}
    rows_list = unique_names.to_dict("records")

    for i in range(0, len(rows_list), BATCH_SIZE):
        batch = rows_list[i:i + BATCH_SIZE]
        results = _query_claude_batch(batch, api_key)
        domain_lookup.update(results)
        if i + BATCH_SIZE < len(rows_list):
            time.sleep(0.5)

    resolved_count = 0
    for idx, row in out.iterrows():
        if out.at[idx, "contractor_domain"]:
            continue
        name = str(row.get("contractor_name_normalized", "")).lower().strip()
        if name in domain_lookup and domain_lookup[name]:
            out.at[idx, "contractor_domain"] = domain_lookup[name]
            out.at[idx, "domain_resolution_source"] = "claude_enrichment"
            resolved_count += 1

    log.info("Claude enrichment resolved %d additional domains", resolved_count)
    return out


def _query_claude_batch(batch: List[dict], api_key: str) -> Dict[str, str]:
    companies_text = "\n".join(
        f"{i+1}. {r['contractor_name_normalized']} ({r.get('project_city','')}, {r.get('project_state','')})"
        for i, r in enumerate(batch)
    )

    prompt = f"""You are a business research assistant. For each contractor below, return their most likely website domain.

Rules:
- Return ONLY a JSON object mapping the exact company name (as given) to its domain
- Domain format: just the domain, no https:// or www (e.g. "beckgroup.com")
- If you are not confident about a company, use null
- Do not guess or hallucinate domains — only return domains you are reasonably confident about
- These are construction/electrical/mechanical contractors in Texas

Companies:
{companies_text}

Return JSON only, no other text. Example:
{{"company name here": "domain.com", "other company": null}}"""

    try:
        resp = requests.post(
            ANTHROPIC_API_URL,
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": MODEL,
                "max_tokens": 1024,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        text = data["content"][0]["text"].strip()

        if text.startswith("```"):
            text = text.split("\n", 1)[1].rsplit("```", 1)[0].strip()

        result_raw = json.loads(text)

        result: Dict[str, str] = {}
        for k, v in result_raw.items():
            norm_key = k.lower().strip()
            if v and isinstance(v, str) and "." in v:
                domain = v.lower().strip().lstrip("www.").rstrip("/")
                result[norm_key] = domain
            else:
                result[norm_key] = ""

        log.info("  Claude batch (%d companies): %d domains resolved",
                 len(batch), sum(1 for v in result.values() if v))
        return result

    except Exception as exc:
        log.warning("Claude API batch failed: %s", exc)
        return {}
