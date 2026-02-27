from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import pandas as pd

from .utils import load_yaml, normalize_text

log = logging.getLogger("cranegenius.score")


def _get_group(kw_cfg: Dict[str, Any], group_path: str) -> List[str]:
    cur: Any = kw_cfg
    for part in group_path.split("."):
        cur = cur.get(part, {})
    if isinstance(cur, list):
        return [normalize_text(x).lower() for x in cur]
    return []


def score_and_filter(df: pd.DataFrame, keywords_yaml: str, scoring_yaml: str) -> pd.DataFrame:
    kw = load_yaml(keywords_yaml).get("keywords", {})
    scoring = load_yaml(scoring_yaml)["scoring"]
    rules = scoring["rules"]

    recency_days = int(scoring.get("recency_days", 30))
    cutoff = datetime.now(timezone.utc).date() - timedelta(days=recency_days)

    out_rows: List[Dict[str, Any]] = []

    for _, r in df.iterrows():
        desc = normalize_text(r.get("description_raw")).lower()
        status = normalize_text(r.get("record_status")).lower()
        date_iso = normalize_text(r.get("record_date_iso"))

        score = 0
        hits: List[str] = []

        for rule in rules:
            pts = int(rule["points"])

            if rule.get("field") == "record_status":
                allowed = [normalize_text(x).lower() for x in rule.get("match_any", [])]
                if status in allowed:
                    score += pts
                    hits.append(rule["name"])
                continue

            if rule.get("field") == "description_raw":
                if "match_group" in rule:
                    phrases = _get_group({"keywords": kw}, "keywords." + rule["match_group"])
                    if any(p and p in desc for p in phrases):
                        score += pts
                        hits.append(rule["name"])

                elif "match_any_groups" in rule:
                    phrases_all: List[str] = []
                    for gp in rule["match_any_groups"]:
                        phrases_all.extend(_get_group({"keywords": kw}, "keywords." + gp))
                    if any(p and p in desc for p in phrases_all):
                        score += pts
                        hits.append(rule["name"])

        recency_ok = False
        if date_iso:
            try:
                d = datetime.fromisoformat(date_iso).date()
                recency_ok = d >= cutoff
            except Exception:
                recency_ok = False

        out = dict(r)
        out["lift_probability_score"] = score + (2 if recency_ok else 0)
        out["score_hits"] = ",".join(hits)
        out["recency_ok"] = recency_ok
        out_rows.append(out)

    result = pd.DataFrame(out_rows)
    hot_count = (result["lift_probability_score"] >= scoring.get("threshold_hot", 7)).sum()
    warm_count = (result["lift_probability_score"] >= scoring.get("threshold_warm", 5)).sum()
    log.info("Scored %d records — hot (7+): %d, warm (5+): %d", len(result), hot_count, warm_count)
    return result
