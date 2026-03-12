# Today Contact System Status (Revenue Focus)

## What Changed

1. Local pipeline input handling (`src/monday_people_pipeline.py`)
- Added `--input-file` CLI arg.
- `_load_inputs(input_file=...)` now supports explicit local CSV input.
- Added local fallback to `companies_priority_200.csv` when Monday source files are absent.
- Improved missing-input error details to list exact checked file paths.
- Kept existing Monday source behavior when standard files exist.

2. Domain recovery improvements (`src/domain_discovery.py`)
- Increased search query budget from 2 to 3 for unresolved companies.
- Kept up to 10 ranked search candidates.
- Lowered conservative score gate from 0.55 to 0.45 to recover more plausible domains.
- Added second-pass search using a slimmed company name (removes common suffix terms) only after normal fallback misses.
- Preserved existing variant + map + validation flow and existing tests.

3. Candidate generation quality (`src/people_email_generator.py`)
- Reordered pattern priority to favor likely human formats first:
  - `first.last`, `flast`, `firstlast`, `f.lastname`, `first_last`, `firstname.lastname`, `first`, `last.first`
- Kept max-pattern coverage and dedupe behavior.
- Kept role inbox handling separate in pipeline outputs.

4. Verification readiness (`src/monday_people_pipeline.py`)
- Retained split outputs:
  - `data/monday_verified_valid.csv`
  - `data/monday_verified_catchall.csv`
  - `data/monday_verified_invalid.csv`
- Added `runs/verification_summary.json` for quick inspection of verification bucket counts/paths.
- Kept `runs/contact_generation_stats.json` output for top-level funnel counts.

## Commands Run

1. `python3 -m unittest discover -s tests -p 'test_domain_discovery.py' -v`
- Result: **PASS** (14/14)

2. `python3 scripts/domain_discovery_benchmark.py --input companies_priority_200.csv --output companies_priority_200_with_domains.csv`
- Started successfully.
- Long-running (network-bound); last completed benchmark output file still shows prior counts.

3. `python3 -m src.monday_people_pipeline --input-file companies_priority_200.csv --max-companies 200`
- Started successfully and entered domain discovery stage.
- Confirms local input path now works (no `No monday input files found` failure).

## Benchmark Before/After

- Before (baseline):
  - `total_companies: 200`
  - `valid_domains: 67`
  - `no_valid_domain: 111`

- After:
  - Latest completed output currently still at:
    - `total_companies: 200`
    - `valid_domains: 67`
    - `no_valid_domain: 111`
  - Note: a new benchmark run is still in progress; final after numbers pending completion.

## Pipeline Local Run Status

- `--input-file companies_priority_200.csv` path works.
- Pipeline now runs locally from the repo root without requiring Monday CSV inputs.

## Remaining Blockers To Producing Verified Contacts

1. Domain discovery remains network-latency bound; full 200-company runs take significant time.
2. A large unresolved bucket (`no_valid_domain`) still limits downstream people discovery volume.
3. MillionVerifier outcome quality (valid vs catchall/invalid) still depends on recovered domain quality and discovered person quality.
4. Contact-page extraction quality is constrained by sparse or JavaScript-heavy sites.
