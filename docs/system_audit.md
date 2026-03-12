# System Audit

- Generated: `2026-03-12T22:02:54.302215+00:00`
- Repo root: `/Users/lemueldavidleejr/Downloads/cranegenius_repo`
- Scope: executable source only (`.py`, `.js`, `.jsx`, `.sh`, backup snapshots). Static HTML/assets/config/data are excluded from the component inventory but still inform pipeline references.
- Verification: `python3 -m unittest discover -s tests -v` -> `passed (61 tests)`

## Executive Summary

- Working: `82`
- Partial: `20`
- Broken: `2`
- Unused: `24`

Named pipeline surfaces discovered:
- `contact_intelligence_v2`: `12` components
- `frontend_runtime`: `8` components
- `intent_pipeline`: `23` components
- `learning_memory`: `6` components
- `llm_cli`: `6` components
- `market_intel`: `7` components
- `monday_people_pipeline`: `10` components
- `project_intelligence`: `6` components
- `test_suite`: `6` components

## Broken Components

- `scripts/review_latest_pipeline_outputs.py`: Cannot be parsed: unterminated triple-quoted string literal (detected at line 465) (line 307).
- `src/pipeline.py`: Cannot be parsed: unexpected indent (line 179).

## Classification Rules

- `Working`: parseable and connected to an active pipeline, runtime surface, or passing test suite.
- `Partial`: parseable standalone utility or adjunct tool without verified pipeline wiring.
- `Broken`: parse failure or obviously non-runnable source in the current repo state.
- `Unused`: backup snapshot, duplicate copy, or orphaned helper with no active wiring discovered in the repo.

## Component Inventory

| Path | Status | Entry Point | Pipeline | Notes |
|---|---|---|---|---|
| `assets/js/page_ai_context.js` | `Working` | `javascript module/script` | `frontend_runtime` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `cloudflare/worker.js` | `Working` | `javascript module/script` | `frontend_runtime` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `config.example.js` | `Unused` | `javascript module/script` | `frontend_runtime` | Standalone helper or migration artifact with no active pipeline references. |
| `config.js` | `Working` | `javascript module/script` | `frontend_runtime` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `contact_intelligence/scripts/add_permit_personalization.py` | `Partial` | `__main__ block` | `-` | Runnable standalone utility, but not wired into a primary pipeline. |
| `contact_intelligence/scripts/backup_db.py` | `Working` | `__main__ block` | `contact_intelligence_v2` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `contact_intelligence/scripts/build_legacy_outbound.py` | `Partial` | `__main__ block` | `-` | Runnable standalone utility, but not wired into a primary pipeline. |
| `contact_intelligence/scripts/build_outbound_candidates.py` | `Partial` | `__main__ block` | `-` | Runnable standalone utility, but not wired into a primary pipeline. |
| `contact_intelligence/scripts/build_project_intelligence.py` | `Working` | `__main__ block` | `project_intelligence` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `contact_intelligence/scripts/dedupe_people_companies.py` | `Working` | `__main__ block` | `contact_intelligence_v2` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `contact_intelligence/scripts/enrich_outbound_quality.py` | `Partial` | `__main__ block` | `-` | Runnable standalone utility, but not wired into a primary pipeline. |
| `contact_intelligence/scripts/export_project_intelligence.py` | `Working` | `__main__ block` | `project_intelligence` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `contact_intelligence/scripts/export_static_json.py` | `Working` | `__main__ block` | `project_intelligence` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `contact_intelligence/scripts/export_tomorrows_1000.py` | `Partial` | `__main__ block` | `-` | Runnable standalone utility, but not wired into a primary pipeline. |
| `contact_intelligence/scripts/export_views.py` | `Working` | `__main__ block` | `contact_intelligence_v2` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `contact_intelligence/scripts/finalize_outbound_copy.py` | `Partial` | `__main__ block` | `-` | Runnable standalone utility, but not wired into a primary pipeline. |
| `contact_intelligence/scripts/import_csv.py` | `Working` | `__main__ block` | `contact_intelligence_v2` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `contact_intelligence/scripts/import_seed_data.py` | `Working` | `__main__ block` | `contact_intelligence_v2` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `contact_intelligence/scripts/init_db.py` | `Working` | `__main__ block` | `contact_intelligence_v2` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `contact_intelligence/scripts/log_feedback.py` | `Working` | `__main__ block` | `contact_intelligence_v2` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `contact_intelligence/scripts/normalize_and_match_feeds.py` | `Working` | `__main__ block` | `project_intelligence` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `contact_intelligence/scripts/normalize_records.py` | `Working` | `__main__ block` | `contact_intelligence_v2` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `contact_intelligence/scripts/reply_from_inbox.py` | `Partial` | `__main__ block` | `-` | Runnable standalone utility, but not wired into a primary pipeline. |
| `contact_intelligence/scripts/run_gold_truth_checks.py` | `Working` | `__main__ block` | `contact_intelligence_v2` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `contact_intelligence/scripts/score_domain_evidence.py` | `Working` | `__main__ block` | `contact_intelligence_v2` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `contact_intelligence/scripts/search_cli.py` | `Working` | `__main__ block` | `contact_intelligence_v2` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `contact_intelligence/scripts/update_contact_patterns.py` | `Working` | `__main__ block` | `contact_intelligence_v2` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `contact_intelligence/scripts/verify_emails_millionverifier.py` | `Partial` | `__main__ block` | `-` | Runnable standalone utility, but not wired into a primary pipeline. |
| `data/opportunities/opportunity_helpers.js` | `Working` | `javascript module/script` | `frontend_runtime` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `data/suppliers/routing_helper.js` | `Working` | `javascript module/script` | `frontend_runtime` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `enrich_ci.py` | `Unused` | `-` | `-` | Standalone helper or migration artifact with no active pipeline references. |
| `enrich_free.py` | `Unused` | `-` | `-` | Standalone helper or migration artifact with no active pipeline references. |
| `filter_to_chicago.py` | `Unused` | `-` | `-` | Standalone helper or migration artifact with no active pipeline references. |
| `fix2.py` | `Unused` | `-` | `-` | Standalone helper or migration artifact with no active pipeline references. |
| `fix_ci.py` | `Unused` | `-` | `-` | Standalone helper or migration artifact with no active pipeline references. |
| `import_apollo.py` | `Unused` | `-` | `-` | Standalone helper or migration artifact with no active pipeline references. |
| `inject.py` | `Unused` | `-` | `-` | Standalone helper or migration artifact with no active pipeline references. |
| `install_contact_intelligence.py` | `Unused` | `__main__ block` | `-` | Standalone helper or migration artifact with no active pipeline references. |
| `intel/01_edgar_scraper.py` | `Working` | `__main__ block` | `market_intel` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `intel/02_epa_turnaround_scraper.py` | `Working` | `__main__ block` | `market_intel` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `intel/03_utility_interconnection_scraper.py` | `Working` | `__main__ block` | `market_intel` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `intel/04_job_posting_scraper.py` | `Working` | `__main__ block` | `market_intel` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `intel/05_blm_mining_solar_scraper.py` | `Working` | `__main__ block` | `market_intel` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `intel/files/01_edgar_scraper.py` | `Unused` | `__main__ block` | `-` | Snapshot or duplicate copy; not part of the active execution surface. |
| `intel/files/02_epa_turnaround_scraper.py` | `Unused` | `__main__ block` | `-` | Snapshot or duplicate copy; not part of the active execution surface. |
| `intel/files/03_utility_interconnection_scraper.py` | `Unused` | `__main__ block` | `-` | Snapshot or duplicate copy; not part of the active execution surface. |
| `intel/files/04_job_posting_scraper.py` | `Unused` | `__main__ block` | `-` | Snapshot or duplicate copy; not part of the active execution surface. |
| `intel/files/05_blm_mining_solar_scraper.py` | `Unused` | `__main__ block` | `-` | Snapshot or duplicate copy; not part of the active execution surface. |
| `intel/files/outreach_today.jsx` | `Unused` | `javascript module/script` | `-` | Snapshot or duplicate copy; not part of the active execution surface. |
| `intel/files/run_all.py` | `Unused` | `__main__ block` | `-` | Snapshot or duplicate copy; not part of the active execution surface. |
| `intel/outreach_today.jsx` | `Working` | `javascript module/script` | `market_intel` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `intel/run_all.py` | `Working` | `__main__ block` | `market_intel` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `opportunities/data_loader.js` | `Working` | `javascript module/script` | `frontend_runtime` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `opportunities/detail.js` | `Working` | `javascript module/script` | `frontend_runtime` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `scripts/add_mini_crane_logic.py` | `Unused` | `__main__ block` | `-` | Standalone helper or migration artifact with no active pipeline references. |
| `scripts/append_experiment.py` | `Working` | `__main__ block` | `learning_memory` | Referenced elsewhere in the repo and has no parse errors. |
| `scripts/append_lesson.py` | `Working` | `__main__ block` | `learning_memory` | Referenced elsewhere in the repo and has no parse errors. |
| `scripts/append_metrics_history.py` | `Working` | `__main__ block` | `learning_memory` | Referenced elsewhere in the repo and has no parse errors. |
| `scripts/append_pipeline_metrics.py` | `Partial` | `__main__ block` | `learning_memory` | Valid source file, but no active pipeline wiring or invocation path was found in the repo. |
| `scripts/append_signal_log.py` | `Working` | `__main__ block` | `learning_memory` | Referenced elsewhere in the repo and has no parse errors. |
| `scripts/apply_merge_now_from_backup.sh` | `Unused` | `shell script` | `-` | Standalone helper or migration artifact with no active pipeline references. |
| `scripts/audit_backup_repo.py` | `Partial` | `__main__ block` | `-` | Runnable standalone utility, but not wired into a primary pipeline. |
| `scripts/audit_page_ai_context.py` | `Partial` | `__main__ block` | `-` | Runnable standalone utility, but not wired into a primary pipeline. |
| `scripts/domain_discovery_benchmark.py` | `Partial` | `__main__ block` | `-` | Runnable standalone utility, but not wired into a primary pipeline. |
| `scripts/review_latest_pipeline_outputs.py` | `Broken` | `-` | `-` | Cannot be parsed: unterminated triple-quoted string literal (detected at line 465) (line 307). Syntax: unterminated triple-quoted string literal (detected at line 465) (line 307). |
| `scripts/run_and_prepare_learning.py` | `Working` | `__main__ block` | `learning_memory` | Referenced elsewhere in the repo and has no parse errors. |
| `scripts/scan_secrets.sh` | `Partial` | `shell script` | `-` | Runnable standalone utility, but not wired into a primary pipeline. |
| `scripts/scrape_jobs_public.py` | `Working` | `__main__ block` | `project_intelligence` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `scripts/scrape_permits_public.py` | `Working` | `__main__ block` | `project_intelligence` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `scripts/smoke_test_apify.py` | `Partial` | `-` | `-` | Runnable standalone utility, but not wired into a primary pipeline. |
| `scripts/smoke_test_apollo.py` | `Partial` | `-` | `-` | Runnable standalone utility, but not wired into a primary pipeline. |
| `scripts/smoke_test_firecrawl.py` | `Partial` | `-` | `-` | Runnable standalone utility, but not wired into a primary pipeline. |
| `scripts/smoke_test_millionverifier.py` | `Partial` | `-` | `-` | Runnable standalone utility, but not wired into a primary pipeline. |
| `scripts/smoke_test_vercel.py` | `Partial` | `-` | `-` | Runnable standalone utility, but not wired into a primary pipeline. |
| `src/__init__.py` | `Working` | `-` | `-` | Referenced elsewhere in the repo and has no parse errors. |
| `src/candidate_builder.py` | `Working` | `-` | `intent_pipeline` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `src/candidate_builder.py.bak.20260308_104658` | `Unused` | `backup snapshot` | `-` | Snapshot or duplicate copy; not part of the active execution surface. |
| `src/cli/__init__.py` | `Working` | `-` | `llm_cli` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `src/cli/__main__.py` | `Working` | `__main__ block` | `llm_cli` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `src/company_resolver.py` | `Working` | `-` | `intent_pipeline` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `src/company_selector.py` | `Working` | `-` | `intent_pipeline` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `src/contact_page_finder.py` | `Working` | `-` | `monday_people_pipeline` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `src/crm_contact_importer.py` | `Working` | `__main__ block` | `monday_people_pipeline` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `src/domain_dedupe.py` | `Working` | `-` | `monday_people_pipeline` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `src/domain_discovery.py` | `Working` | `-` | `monday_people_pipeline` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `src/domain_enricher_claude.py` | `Working` | `-` | `intent_pipeline` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `src/exporter.py` | `Working` | `-` | `intent_pipeline` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `src/google_domain_enricher.py` | `Partial` | `__main__ block` | `-` | Runnable standalone utility, but not wired into a primary pipeline. |
| `src/ingest.py` | `Working` | `-` | `intent_pipeline` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `src/ingest.py.bak.20260308_104658` | `Unused` | `backup snapshot` | `-` | Snapshot or duplicate copy; not part of the active execution surface. |
| `src/llm/__init__.py` | `Working` | `-` | `llm_cli` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `src/llm/context_loader.py` | `Working` | `-` | `llm_cli` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `src/llm/router.py` | `Working` | `-` | `llm_cli` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `src/llm/schemas.py` | `Working` | `-` | `llm_cli` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `src/monday_campaign_fast_path.py` | `Working` | `__main__ block` | `monday_people_pipeline` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `src/monday_company_list_fast_path.py` | `Working` | `__main__ block` | `monday_people_pipeline` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `src/monday_individual_contact_generation.py` | `Working` | `__main__ block` | `monday_people_pipeline` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `src/monday_people_pipeline.py` | `Working` | `__main__ block` | `monday_people_pipeline` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `src/monitor.py` | `Working` | `-` | `intent_pipeline` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `src/parse_normalize.py` | `Working` | `-` | `intent_pipeline` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `src/parse_normalize.py.bak.20260308_104658` | `Unused` | `backup snapshot` | `-` | Snapshot or duplicate copy; not part of the active execution surface. |
| `src/people_discovery.py` | `Working` | `-` | `monday_people_pipeline` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `src/people_email_generator.py` | `Working` | `-` | `monday_people_pipeline` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `src/pipeline.py` | `Broken` | `-` | `intent_pipeline` | Cannot be parsed: unexpected indent (line 179). Syntax: unexpected indent (line 179). |
| `src/pipeline.py.bak.20260308_104658` | `Unused` | `backup snapshot` | `-` | Snapshot or duplicate copy; not part of the active execution surface. |
| `src/score_filter.py` | `Working` | `-` | `intent_pipeline` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `src/score_filter.py.bak.20260308_104658` | `Unused` | `backup snapshot` | `-` | Snapshot or duplicate copy; not part of the active execution surface. |
| `src/scrapers/__init__.py` | `Working` | `-` | `intent_pipeline` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `src/scrapers/__init__.py.bak.20260308_104658` | `Unused` | `backup snapshot` | `-` | Snapshot or duplicate copy; not part of the active execution surface. |
| `src/scrapers/bid_board_scraper.py` | `Working` | `-` | `intent_pipeline` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `src/scrapers/chicago.py` | `Working` | `-` | `intent_pipeline` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `src/scrapers/contractor_directory_scraper.py` | `Working` | `-` | `intent_pipeline` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `src/scrapers/dallas.py` | `Working` | `-` | `intent_pipeline` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `src/scrapers/industrial_project_scraper.py` | `Working` | `-` | `intent_pipeline` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `src/scrapers/nyc.py` | `Working` | `-` | `intent_pipeline` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `src/scrapers/permit_multi_city_scraper.py` | `Working` | `-` | `intent_pipeline` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `src/scrapers/phoenix.py` | `Working` | `-` | `intent_pipeline` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `src/sheets_exporter.py` | `Working` | `-` | `intent_pipeline` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `src/site_contact_miner.py` | `Working` | `-` | `intent_pipeline` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `src/stage9_email_writer.py` | `Partial` | `__main__ block` | `-` | Runnable standalone utility, but not wired into a primary pipeline. |
| `src/utils.py` | `Working` | `-` | `intent_pipeline` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `src/verify_millionverifier.py` | `Working` | `-` | `intent_pipeline` | Connected to a named pipeline or runtime surface and has no parse errors. |
| `tests/test_crm_contact_importer.py` | `Working` | `__main__ block` | `test_suite` | Included in the test suite; `python3 -m unittest discover -s tests -v` passed. |
| `tests/test_domain_discovery.py` | `Working` | `__main__ block` | `test_suite` | Included in the test suite; `python3 -m unittest discover -s tests -v` passed. |
| `tests/test_monday_people_pipeline.py` | `Working` | `__main__ block` | `test_suite` | Included in the test suite; `python3 -m unittest discover -s tests -v` passed. |
| `tests/test_parse_normalize.py` | `Working` | `__main__ block` | `test_suite` | Included in the test suite; `python3 -m unittest discover -s tests -v` passed. |
| `tests/test_people_discovery.py` | `Working` | `__main__ block` | `test_suite` | Included in the test suite; `python3 -m unittest discover -s tests -v` passed. |
| `tests/test_people_email_generator.py` | `Working` | `__main__ block` | `test_suite` | Included in the test suite; `python3 -m unittest discover -s tests -v` passed. |

## Notes

- `src/pipeline.py` is still intended as the classic intent-pipeline orchestrator, but the current file does not parse because of an indentation error.
- `src/cli/__main__.py` and `src/monday_people_pipeline.py` form a newer operational surface and both parsed cleanly during this audit.
- `contact_intelligence/` is a separate SQLite-backed system with its own documented workflow and appears internally coherent from the repo structure.
- `intel/files/` duplicates `intel/` and was classified as `Unused` to avoid double-counting active components.
