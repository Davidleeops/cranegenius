#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.domain_discovery import discover_company_domains


def main() -> int:
    parser = argparse.ArgumentParser(description="Benchmark domain discovery outcomes for a company CSV.")
    parser.add_argument("--input", required=True, help="Input CSV path (must include contractor_name_normalized).")
    parser.add_argument(
        "--output",
        default="companies_with_domains_benchmark.csv",
        help="Output CSV path for enriched domain results.",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        raise SystemExit(f"Input file not found: {input_path}")

    df = pd.read_csv(input_path)
    out = discover_company_domains(df)
    out.to_csv(args.output, index=False)

    total = int(len(out))
    valid = int(out["domain_valid"].fillna(False).astype(bool).sum()) if "domain_valid" in out.columns else 0
    valid_mx = (
        int((out["domain_valid"].fillna(False).astype(bool) & out["mx_valid"].fillna(False).astype(bool)).sum())
        if {"domain_valid", "mx_valid"}.issubset(out.columns)
        else 0
    )

    print(f"input_file: {input_path}")
    print(f"output_file: {Path(args.output).resolve()}")
    print(f"total_companies: {total}")
    print(f"valid_domains: {valid}")
    print(f"valid_plus_mx: {valid_mx}")

    if "domain_validation_reason" in out.columns:
        print("\nreason_counts:")
        counts = out["domain_validation_reason"].fillna("unknown").astype(str).value_counts()
        for reason, count in counts.items():
            print(f"  {reason}: {int(count)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
