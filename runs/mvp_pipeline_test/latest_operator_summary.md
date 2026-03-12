# MVP Pipeline Run Summary

- Generated: `2026-03-12T22:17:37.690845+00:00`
- Overall status: `success`
- Database: `/tmp/cranegenius_mvp_test.db`
- Export directory: `/tmp/cranegenius_mvp_exports`

## Stage Status

- `init_db`: `success` in `0.06s`
- `scrape_permits`: `success` in `6.41s`
- `scrape_jobs`: `success` in `50.04s`
- `normalize_match`: `success` in `0.07s`
- `build_project_intelligence`: `success` in `0.06s`
- `export_project_intelligence`: `success` in `0.25s`

## Business Output

- Top project candidates: `20`
- Recommended expansion candidates: `3`
- Project candidate rows in DB: `26`
- Signal events in DB: `26`

## Next Operator Check

- Review `/tmp/cranegenius_mvp_exports`
- Review `/Users/lemueldavidleejr/Downloads/cranegenius_repo/runs/mvp_pipeline_test/20260312T221640Z/run_summary.json` for stage-level validations and metrics
