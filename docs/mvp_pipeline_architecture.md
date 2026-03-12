# MVP Pipeline Architecture

## Chosen End-to-End Flow

The MVP orchestrator standardizes the project-intelligence path because it is the strongest currently working lane in the repo.

Ordered stages:
1. `contact_intelligence/scripts/init_db.py`
2. `scripts/scrape_permits_public.py`
3. `scripts/scrape_jobs_public.py`
4. `contact_intelligence/scripts/normalize_and_match_feeds.py`
5. `contact_intelligence/scripts/build_project_intelligence.py`
6. `contact_intelligence/scripts/export_project_intelligence.py`
7. `scripts/run_mvp_pipeline.py` writes run summaries and validation output

## Why This Flow

- It uses working audited entry points.
- It avoids the broken legacy orchestrator in `src/pipeline.py`.
- It already supports both operator-facing exports and machine-readable JSON.
- It has a deterministic SQLite-backed middle layer instead of ad hoc file chaining only.

## Stage Interfaces

| Stage | Inputs | Outputs |
|---|---|---|
| `init_db` | schema files under `contact_intelligence/schema/` | SQLite DB at `~/data_runtime/cranegenius_ci.db` by default |
| `scrape_permits` | `config/permit_sources.json` | `data/opportunities/permits_imported.json` |
| `scrape_jobs` | optional `data/jobs_manual_import.csv`, live public feeds | `data/jobs_imported.json` |
| `normalize_match` | DB, `data/jobs_imported.json`, `data/opportunities/permits_imported.json` | normalized/match rows in SQLite tables |
| `build_project_intelligence` | DB plus repo data files | `signal_events`, `project_candidates`, related SQLite tables |
| `export_project_intelligence` | DB | operator exports in `~/data_runtime/exports`, static JSON in `data/static_exports/` |
| `run_mvp_pipeline` | all stage definitions above | `runs/mvp_pipeline/<run_id>/run_summary.json`, `operator_summary.md`, `pipeline.log` |

## Explicit Output Contract

The orchestrator treats these as required outputs:
- `data/opportunities/permits_imported.json`
- `data/jobs_imported.json`
- SQLite DB file
- `data/static_exports/project_candidates.json`
- `data/static_exports/top_project_candidates.json`

Optional outputs:
- spreadsheet or CSV exports in `~/data_runtime/exports`
- additional static JSON exports such as mini-opportunity or expansion candidates

## Failure Model

A stage fails if either condition is true:
- subprocess exits non-zero
- required outputs for that stage are missing after execution

When a failure occurs, the orchestrator stops and writes:
- run log
- machine-readable summary
- stage-level failure reason

## Operator Surface

Operators should not read raw logs first. They should check:
1. `runs/mvp_pipeline/latest_operator_summary.md`
2. `runs/mvp_pipeline/latest_run_summary.json`
3. export files in `~/data_runtime/exports`
