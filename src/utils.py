from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
import tldextract
import yaml

log = logging.getLogger("cranegenius")

EMAIL_RE = re.compile(r"\b[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}\b")
PHONE_RE = re.compile(r"(\+?\d[\d\-\(\)\s]{8,}\d)")
WHITESPACE_RE = re.compile(r"\s+")


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
        datefmt="%H:%M:%S",
    )


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def load_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def save_csv(df: pd.DataFrame, path: str) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    df.to_csv(path, index=False)
    log.info("Saved %d rows → %s", len(df), path)


def read_csv(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        log.warning("File not found: %s — returning empty DataFrame", path)
        return pd.DataFrame()
    return pd.read_csv(path)


def normalize_text(s: Optional[Any]) -> str:
    if s is None or (isinstance(s, float) and s != s):  # NaN check
        return ""
    return WHITESPACE_RE.sub(" ", str(s)).strip()


def domain_from_url(url: str) -> str:
    ext = tldextract.extract(url)
    if not ext.domain or not ext.suffix:
        return ""
    return f"{ext.domain}.{ext.suffix}".lower()


def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def rate_limit_sleep(seconds: float) -> None:
    if seconds and seconds > 0:
        time.sleep(seconds)


def extract_emails(text: str) -> List[str]:
    return sorted(set(m.group(0).lower() for m in EMAIL_RE.finditer(text or "")))


def extract_phones(text: str) -> List[str]:
    phones = []
    for m in PHONE_RE.finditer(text or ""):
        phones.append(normalize_text(m.group(0)))
    return sorted(set(phones))


def safe_json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
