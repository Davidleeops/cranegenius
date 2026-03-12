# MVP Pipeline Runbook

## Purpose

Run the current CraneGenius MVP flow end to end and produce:
- operator-ready exports
- machine-readable JSON outputs
- a run summary with stage-level visibility

## Command

```bash
python3 scripts/run_mvp_pipeline.py
```

Optional overrides:

```bash
python3 scripts/run_mvp_pipeline.py \
  --db ~/data_runtime/cranegenius_ci.db \
  --export-dir ~/data_runtime/exports \
  --json-output-dir data \
  --runs-dir runs/mvp_pipeline
```

## Expected Outputs

### Operator-facing
- `~/data_runtime/exports/top_project_candidates.xlsx` or `.csv`
- `~/data_runtime/exports/recommended_expansion_candidates.xlsx` or `.csv`
- `runs/mvp_pipeline/latest_operator_summary.md`

### Machine-readable
- `data/static_exports/project_candidates.json`
- `data/static_exports/top_project_candidates.json`
- `data/static_exports/mini_opportunity_candidates.json`
- `data/static_exports/recommended_expansion_candidates.json`
- `runs/mvp_pipeline/latest_run_summary.json`

### Logs
- `runs/mvp_pipeline/latest_pipeline.log`
- `runs/mvp_pipeline/<run_id>/pipeline.log`

## What Success Looks Like

A successful run means:
- all six stages complete
- required JSON outputs exist
- database exists and contains project candidate rows
- run summary status is `success`

## What To Check If It Fails

1. Open `runs/mvp_pipeline/latest_run_summary.json`
2. Find the first stage with status `failed`
3. Read that stage's `stderr`, `stdout`, and `failure_reason`
4. Confirm the expected required outputs exist

## Known Risk Areas

- external feeds may return zero rows or fail transiently
- spreadsheet export format depends on `openpyxl`; CSV is used if not installed
- SQLite DB must be writable at the configured path
- legacy `src/pipeline.py` is not part of this run path

## Operational Notes

- permit and jobs scrapers preserve previous outputs when new external results are empty
- this flow is designed for MVP operation, not full production hardening yet
- run summaries are intended to let a non-developer determine whether the pipeline completed and where it broke
