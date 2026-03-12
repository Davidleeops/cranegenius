from __future__ import annotations

import argparse
import logging
import re
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

from .domain_discovery import clean_company_name
from urllib.parse import urlparse

from .utils import normalize_text, setup_logging

log = logging.getLogger("cranegenius.crm_contact_importer")

DEFAULT_RAW_DIR = Path("data") / "imported_contact_sources" / "raw_imports"
DEFAULT_OUT_DIR = Path("data") / "imported_contact_sources"
DEFAULT_NORMALIZED_OUT = DEFAULT_OUT_DIR / "normalized_contacts.csv"
DEFAULT_SEEDS_OUT = DEFAULT_OUT_DIR / "company_domain_seed_enriched.csv"
DEFAULT_CONFLICTS_OUT = DEFAULT_OUT_DIR / "contact_conflicts.csv"
DEFAULT_SUMMARY_OUT = Path("runs") / "import_summary.md"

EMAIL_DOMAIN_RE = re.compile(r"^[a-z0-9.-]+\.[a-z]{2,}$")

COMMON_SECOND_LEVEL_SUFFIXES = {"co.uk", "org.uk", "ac.uk", "com.au", "com.br", "co.jp", "com.mx", "co.nz", "com.sg"}

FREE_ISP_DOMAINS = {
    "gmail.com",
    "googlemail.com",
    "yahoo.com",
    "yahoo.co.uk",
    "outlook.com",
    "hotmail.com",
    "live.com",
    "msn.com",
    "icloud.com",
    "me.com",
    "mac.com",
    "aol.com",
    "proton.me",
    "protonmail.com",
    "pm.me",
    "gmx.com",
    "mail.com",
    "zoho.com",
    "yandex.com",
    "qq.com",
    "163.com",
    "126.com",
    "sbcglobal.net",
    "att.net",
    "verizon.net",
    "comcast.net",
    "cox.net",
    "earthlink.net",
    "charter.net",
}

FREE_ISP_SUFFIXES = (
    ".rr.com",
    ".bellsouth.net",
    ".btinternet.com",
)

SUSPICIOUS_GENERIC_DOMAINS = {
    "example.com",
    "example.org",
    "example.net",
    "test.com",
    "invalid",
    "localhost",
}

GENERIC_COMPANY_NAMES = {
    "",
    "na",
    "n/a",
    "none",
    "unknown",
    "self",
    "individual",
    "no company",
}

NORMALIZED_COLUMNS = [
    "source_file",
    "source_sheet",
    "source_row",
    "raw_company_name",
    "cleaned_company_name",
    "first_name",
    "last_name",
    "full_name",
    "job_title",
    "department",
    "email",
    "email_domain",
    "email_domain_type",
    "email_verification_result",
    "company_website",
    "domain_from_website",
    "preferred_domain",
    "phone_number",
    "city",
    "state",
    "country",
    "company_seed_candidate",
    "company_confidence",
    "person_confidence",
]

SEED_COLUMNS = [
    "cleaned_company_name",
    "preferred_domain",
    "supporting_rows_count",
    "corporate_email_count",
    "free_email_count",
    "confidence_score",
    "conflict_flag",
]

CONFLICT_COLUMNS = [
    "conflict_type",
    "cleaned_company_name",
    "preferred_domain",
    "source_file",
    "source_sheet",
    "source_row",
    "email",
    "notes",
]


def _normalize_domain(domain: str) -> str:
    value = normalize_text(domain).strip().lower().strip(".")
    return value


def _domain_from_email(email: str) -> str:
    value = normalize_text(email).lower().strip()
    if "@" not in value:
        return ""
    parts = value.rsplit("@", 1)
    domain = _normalize_domain(parts[1])
    if not EMAIL_DOMAIN_RE.match(domain):
        return ""
    return domain


def _registered_domain_from_host(host: str) -> str:
    clean_host = _normalize_domain(host.replace("www.", ""))
    if not clean_host:
        return ""
    labels = [label for label in clean_host.split(".") if label]
    if len(labels) < 2:
        return ""

    candidate_suffix = ".".join(labels[-2:])
    if len(labels) >= 3 and candidate_suffix in COMMON_SECOND_LEVEL_SUFFIXES:
        return ".".join(labels[-3:])
    return ".".join(labels[-2:])


def _domain_from_website(website: str) -> str:
    raw = normalize_text(website).strip().lower()
    if not raw:
        return ""
    if not raw.startswith(("http://", "https://")):
        raw = f"https://{raw}"
    parsed = urlparse(raw)
    host = normalize_text(parsed.netloc or parsed.path).split("/", 1)[0]
    return _registered_domain_from_host(host)


def _is_free_or_isp_domain(domain: str) -> bool:
    clean = _normalize_domain(domain)
    if not clean:
        return False
    if clean in FREE_ISP_DOMAINS:
        return True
    return any(clean.endswith(suffix) for suffix in FREE_ISP_SUFFIXES)


def _is_suspicious_domain(domain: str) -> bool:
    clean = _normalize_domain(domain)
    if not clean:
        return False
    if clean in SUSPICIOUS_GENERIC_DOMAINS:
        return True
    if clean.startswith("example") or clean.startswith("test"):
        return True
    return False


def _clean_company_name(raw_company_name: str) -> str:
    cleaned = clean_company_name(raw_company_name)
    if cleaned in GENERIC_COMPANY_NAMES:
        return ""
    return cleaned


def _full_name(first_name: str, last_name: str) -> str:
    first = normalize_text(first_name)
    last = normalize_text(last_name)
    return normalize_text(f"{first} {last}")


def _company_confidence(cleaned_company_name: str, domain_from_website: str, preferred_domain: str, email_domain_type: str) -> float:
    score = 0.0
    if cleaned_company_name:
        score += 0.45
    if domain_from_website:
        score += 0.35
    elif preferred_domain and email_domain_type == "corporate":
        score += 0.25
    if preferred_domain:
        score += 0.10
    return round(min(score, 1.0), 3)


def _person_confidence(first_name: str, last_name: str, email: str, job_title: str) -> float:
    score = 0.0
    if normalize_text(first_name) and normalize_text(last_name):
        score += 0.40
    if _domain_from_email(email):
        score += 0.35
    if normalize_text(job_title):
        score += 0.15
    if normalize_text(email):
        score += 0.10
    return round(min(score, 1.0), 3)


def _read_raw_file(path: Path) -> List[Tuple[str, pd.DataFrame]]:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return [("csv", pd.read_csv(path, dtype=str).fillna(""))]
    if suffix in {".xlsx", ".xlsm", ".xls"}:
        sheets = pd.read_excel(path, sheet_name=None, dtype=str)  # type: ignore[call-overload]
        return [(str(sheet_name), df.fillna("")) for sheet_name, df in sheets.items()]
    return []


def _iter_normalized_rows(raw_dir: Path) -> Tuple[List[Dict[str, object]], List[str]]:
    rows: List[Dict[str, object]] = []
    processed_files: List[str] = []
    if not raw_dir.exists():
        return rows, processed_files

    files = sorted([p for p in raw_dir.iterdir() if p.is_file() and p.suffix.lower() in {".csv", ".xlsx", ".xls", ".xlsm"}])
    for path in files:
        sheets = _read_raw_file(path)
        if not sheets:
            continue
        processed_files.append(path.name)
        for sheet_name, df in sheets:
            for idx, rec in df.iterrows():
                raw_company_name = normalize_text(rec.get("company_name", ""))
                cleaned_company_name = _clean_company_name(raw_company_name)

                first_name = normalize_text(rec.get("first_name", ""))
                last_name = normalize_text(rec.get("last_name", ""))
                email = normalize_text(rec.get("email", "")).lower()
                email_domain = _domain_from_email(email)
                website = normalize_text(rec.get("company_website", "")).lower()
                domain_from_website = _domain_from_website(website)

                if domain_from_website:
                    preferred_domain = domain_from_website
                elif email_domain and not _is_free_or_isp_domain(email_domain):
                    preferred_domain = email_domain
                else:
                    preferred_domain = ""

                if not email_domain:
                    email_domain_type = "unknown"
                elif _is_free_or_isp_domain(email_domain):
                    email_domain_type = "free_or_isp"
                else:
                    email_domain_type = "corporate"

                company_seed_candidate = bool(cleaned_company_name and preferred_domain)
                company_confidence = _company_confidence(
                    cleaned_company_name=cleaned_company_name,
                    domain_from_website=domain_from_website,
                    preferred_domain=preferred_domain,
                    email_domain_type=email_domain_type,
                )
                person_confidence = _person_confidence(
                    first_name=first_name,
                    last_name=last_name,
                    email=email,
                    job_title=normalize_text(rec.get("job_title", "")),
                )

                rows.append(
                    {
                        "source_file": path.name,
                        "source_sheet": sheet_name,
                        "source_row": int(idx) + 2,
                        "raw_company_name": raw_company_name,
                        "cleaned_company_name": cleaned_company_name,
                        "first_name": first_name,
                        "last_name": last_name,
                        "full_name": _full_name(first_name, last_name),
                        "job_title": normalize_text(rec.get("job_title", "")),
                        "department": normalize_text(rec.get("department", "")),
                        "email": email,
                        "email_domain": email_domain,
                        "email_domain_type": email_domain_type,
                        "email_verification_result": normalize_text(rec.get("email_verification_result", "")).lower(),
                        "company_website": website,
                        "domain_from_website": domain_from_website,
                        "preferred_domain": preferred_domain,
                        "phone_number": normalize_text(rec.get("phone_number", "")),
                        "city": normalize_text(rec.get("city", "")),
                        "state": normalize_text(rec.get("state", "")),
                        "country": normalize_text(rec.get("country", "")),
                        "company_seed_candidate": bool(company_seed_candidate),
                        "company_confidence": company_confidence,
                        "person_confidence": person_confidence,
                    }
                )
    return rows, processed_files


def _build_seed_enriched(normalized_df: pd.DataFrame) -> pd.DataFrame:
    if normalized_df.empty:
        return pd.DataFrame(columns=SEED_COLUMNS)

    seed_input = normalized_df[
        (normalized_df["company_seed_candidate"].astype(bool))
        & (normalized_df["cleaned_company_name"].fillna("") != "")
        & (normalized_df["preferred_domain"].fillna("") != "")
    ].copy()

    if seed_input.empty:
        return pd.DataFrame(columns=SEED_COLUMNS)

    grouped = (
        seed_input.groupby(["cleaned_company_name", "preferred_domain"], dropna=False)
        .agg(
            supporting_rows_count=("preferred_domain", "size"),
            corporate_email_count=("email_domain_type", lambda s: int((s == "corporate").sum())),
            free_email_count=("email_domain_type", lambda s: int((s == "free_or_isp").sum())),
        )
        .reset_index()
    )

    grouped["confidence_score"] = (
        0.25
        + grouped["supporting_rows_count"].clip(upper=5) * 0.12
        + (grouped["corporate_email_count"] > 0).astype(float) * 0.20
        + (grouped["corporate_email_count"] >= 2).astype(float) * 0.20
        - (grouped["free_email_count"] > grouped["corporate_email_count"]).astype(float) * 0.15
    ).clip(lower=0.0, upper=1.0).round(3)

    company_conflicts = grouped.groupby("cleaned_company_name")["preferred_domain"].transform("nunique") > 1
    domain_conflicts = grouped.groupby("preferred_domain")["cleaned_company_name"].transform("nunique") > 1
    grouped["conflict_flag"] = (company_conflicts | domain_conflicts)

    return grouped[SEED_COLUMNS].sort_values(by=["cleaned_company_name", "preferred_domain"]).reset_index(drop=True)


def _build_conflicts(normalized_df: pd.DataFrame) -> pd.DataFrame:
    out_rows: List[Dict[str, object]] = []
    if normalized_df.empty:
        return pd.DataFrame(columns=CONFLICT_COLUMNS)

    work = normalized_df.copy()

    corporate = work[(work["email_domain_type"] == "corporate") & (work["preferred_domain"] != "") & (work["cleaned_company_name"] != "")]

    company_domains = corporate.groupby("cleaned_company_name")["preferred_domain"].nunique()
    for company in company_domains[company_domains > 1].index.tolist():
        company_rows = corporate[corporate["cleaned_company_name"] == company]
        domains = sorted(company_rows["preferred_domain"].dropna().astype(str).unique().tolist())
        out_rows.append(
            {
                "conflict_type": "company_multiple_corporate_domains",
                "cleaned_company_name": company,
                "preferred_domain": "|".join(domains),
                "source_file": "",
                "source_sheet": "",
                "source_row": "",
                "email": "",
                "notes": f"{company} has {len(domains)} corporate domains",
            }
        )

    domain_companies = corporate.groupby("preferred_domain")["cleaned_company_name"].nunique()
    for domain in domain_companies[domain_companies > 1].index.tolist():
        domain_rows = corporate[corporate["preferred_domain"] == domain]
        companies = sorted(domain_rows["cleaned_company_name"].dropna().astype(str).unique().tolist())
        out_rows.append(
            {
                "conflict_type": "domain_used_by_multiple_companies",
                "cleaned_company_name": "|".join(companies),
                "preferred_domain": domain,
                "source_file": "",
                "source_sheet": "",
                "source_row": "",
                "email": "",
                "notes": f"{domain} appears under {len(companies)} companies",
            }
        )

    missing_domain_rows = work[work["preferred_domain"].fillna("") == ""]
    for _, row in missing_domain_rows.iterrows():
        out_rows.append(
            {
                "conflict_type": "missing_usable_domain",
                "cleaned_company_name": normalize_text(row.get("cleaned_company_name", "")),
                "preferred_domain": "",
                "source_file": normalize_text(row.get("source_file", "")),
                "source_sheet": normalize_text(row.get("source_sheet", "")),
                "source_row": row.get("source_row", ""),
                "email": normalize_text(row.get("email", "")),
                "notes": "No website domain and no corporate email domain",
            }
        )

    suspicious_rows = work[
        work["preferred_domain"].map(_is_suspicious_domain).fillna(False)
        | work["email_domain"].map(_is_suspicious_domain).fillna(False)
    ]
    for _, row in suspicious_rows.iterrows():
        out_rows.append(
            {
                "conflict_type": "suspicious_generic_domain",
                "cleaned_company_name": normalize_text(row.get("cleaned_company_name", "")),
                "preferred_domain": normalize_text(row.get("preferred_domain", "")),
                "source_file": normalize_text(row.get("source_file", "")),
                "source_sheet": normalize_text(row.get("source_sheet", "")),
                "source_row": row.get("source_row", ""),
                "email": normalize_text(row.get("email", "")),
                "notes": "Generic/test domain detected",
            }
        )

    out = pd.DataFrame(out_rows, columns=CONFLICT_COLUMNS)
    if out.empty:
        return out
    return out.drop_duplicates().reset_index(drop=True)


def _write_summary(
    *,
    summary_out: Path,
    processed_files: List[str],
    normalized_df: pd.DataFrame,
    seed_df: pd.DataFrame,
    conflicts_df: pd.DataFrame,
) -> None:
    files_processed = len(processed_files)
    rows_processed = int(len(normalized_df))
    rows_with_company_names = int(normalized_df["cleaned_company_name"].fillna("").ne("").sum()) if not normalized_df.empty else 0
    corporate_rows = int((normalized_df["email_domain_type"] == "corporate").sum()) if not normalized_df.empty else 0
    free_rows = int((normalized_df["email_domain_type"] == "free_or_isp").sum()) if not normalized_df.empty else 0
    unique_companies = int(normalized_df["cleaned_company_name"].fillna("").replace("", pd.NA).dropna().nunique()) if not normalized_df.empty else 0
    unique_domains = int(normalized_df["preferred_domain"].fillna("").replace("", pd.NA).dropna().nunique()) if not normalized_df.empty else 0
    conflicts_found = int(len(conflicts_df))

    lines = [
        "# CRM Import Summary",
        "",
        f"- files processed: {files_processed}",
        f"- rows processed: {rows_processed}",
        f"- rows with company names: {rows_with_company_names}",
        f"- rows with corporate email domains: {corporate_rows}",
        f"- rows with free/ISP email domains: {free_rows}",
        f"- unique cleaned companies: {unique_companies}",
        f"- unique preferred domains: {unique_domains}",
        f"- conflicts found: {conflicts_found}",
        "",
        "## Files",
    ]
    if processed_files:
        lines.extend([f"- {name}" for name in processed_files])
    else:
        lines.append("- none")

    summary_out.parent.mkdir(parents=True, exist_ok=True)
    summary_out.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_import(
    raw_dir: Path = DEFAULT_RAW_DIR,
    normalized_out: Path = DEFAULT_NORMALIZED_OUT,
    seeds_out: Path = DEFAULT_SEEDS_OUT,
    conflicts_out: Path = DEFAULT_CONFLICTS_OUT,
    summary_out: Path = DEFAULT_SUMMARY_OUT,
) -> Dict[str, int]:
    raw_dir.mkdir(parents=True, exist_ok=True)
    normalized_out.parent.mkdir(parents=True, exist_ok=True)
    conflicts_out.parent.mkdir(parents=True, exist_ok=True)
    seeds_out.parent.mkdir(parents=True, exist_ok=True)

    rows, processed_files = _iter_normalized_rows(raw_dir)
    normalized_df = pd.DataFrame(rows, columns=NORMALIZED_COLUMNS)
    seed_df = _build_seed_enriched(normalized_df)
    conflicts_df = _build_conflicts(normalized_df)

    normalized_df.to_csv(normalized_out, index=False)
    seed_df.to_csv(seeds_out, index=False)
    conflicts_df.to_csv(conflicts_out, index=False)

    _write_summary(
        summary_out=summary_out,
        processed_files=processed_files,
        normalized_df=normalized_df,
        seed_df=seed_df,
        conflicts_df=conflicts_df,
    )

    stats = {
        "files_processed": len(processed_files),
        "rows_processed": int(len(normalized_df)),
        "rows_with_company_names": int(normalized_df["cleaned_company_name"].fillna("").ne("").sum()) if not normalized_df.empty else 0,
        "rows_with_corporate_domains": int((normalized_df["email_domain_type"] == "corporate").sum()) if not normalized_df.empty else 0,
        "rows_with_free_isp_domains": int((normalized_df["email_domain_type"] == "free_or_isp").sum()) if not normalized_df.empty else 0,
        "unique_cleaned_companies": int(normalized_df["cleaned_company_name"].fillna("").replace("", pd.NA).dropna().nunique()) if not normalized_df.empty else 0,
        "unique_preferred_domains": int(normalized_df["preferred_domain"].fillna("").replace("", pd.NA).dropna().nunique()) if not normalized_df.empty else 0,
        "conflicts_found": int(len(conflicts_df)),
    }
    log.info("CRM import complete: %s", stats)
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="CRM contact source importer")
    parser.add_argument("--raw-dir", type=str, default=str(DEFAULT_RAW_DIR), help="Directory containing raw CSV/XLSX imports")
    parser.add_argument("--normalized-out", type=str, default=str(DEFAULT_NORMALIZED_OUT), help="Output CSV path for normalized contacts")
    parser.add_argument("--seeds-out", type=str, default=str(DEFAULT_SEEDS_OUT), help="Output CSV path for company-domain seed pairs")
    parser.add_argument("--conflicts-out", type=str, default=str(DEFAULT_CONFLICTS_OUT), help="Output CSV path for conflict rows")
    parser.add_argument("--summary-out", type=str, default=str(DEFAULT_SUMMARY_OUT), help="Output markdown path for import summary")
    args = parser.parse_args()

    setup_logging()
    run_import(
        raw_dir=Path(args.raw_dir),
        normalized_out=Path(args.normalized_out),
        seeds_out=Path(args.seeds_out),
        conflicts_out=Path(args.conflicts_out),
        summary_out=Path(args.summary_out),
    )


if __name__ == "__main__":
    main()
