# Repo Backup Merge Plan

## Scope
Source audit: `runs/repo_backup_audit.md`
Goal: preserve important code/config while avoiding accidental overwrite of generated artifacts.

## Merge Now
These are likely high-value source files and should be merged first.

### Config
- `config/routing_schema.json`
- `config/send_selection.yaml`
- `config/signal_schema.json`
- `config/supplier_schema.json`

### Core Source (new in backup)
- `src/company_selector.py`
- `src/domain_dedupe.py`
- `src/domain_discovery.py`
- `src/monday_campaign_fast_path.py`
- `src/monday_company_list_fast_path.py`
- `src/monday_individual_contact_generation.py`
- `src/monday_people_pipeline.py`
- `src/people_discovery.py`
- `src/people_email_generator.py`
- `src/scrapers/bid_board_scraper.py`
- `src/scrapers/contractor_directory_scraper.py`
- `src/scrapers/industrial_project_scraper.py`
- `src/scrapers/permit_multi_city_scraper.py`

### Scripts/Tests (new in backup)
- `scripts/domain_discovery_benchmark.py`
- `scripts/run_and_prepare_learning.py`
- `tests/test_domain_discovery.py`
- `tests/test_people_email_generator.py`

### Existing files that differ (code-critical)
- `src/candidate_builder.py`
- `src/ingest.py`
- `src/parse_normalize.py`
- `src/pipeline.py`
- `src/score_filter.py`
- `src/scrapers/__init__.py`

## Review Manually
These can matter, but should be reviewed by intent before merge.

### Ops/docs/meta
- `.gitignore`
- `CHANGELOG_AGENT.md`
- `TASK.md`

### Frontend/content pages (new in backup)
- `claims/index.html`
- `dashboard/index.html`
- `deals/index.html`
- `outreach/index.html`
- `queue/index.html`
- `send-queue/index.html`
- `signals/index.html`
- `triage/index.html`
- `routing/index.html` (diff)
- `opportunities/detail.js` (diff)

### Data helper JS/readmes/seeds (new in backup)
- `data/agents/agent_loader.js`
- `data/agents/agents_seed.json`
- `data/agents/ai_triage_helper.js`
- `data/agents/approval_queue_helper.js`
- `data/agents/claim_helper.js`
- `data/agents/send_queue_helper.js`
- `data/dashboard/dashboard_helper.js`
- `data/deals/deal_helper.js`
- `data/outreach/outreach_helper.js`
- `data/routing/README.md`
- `data/routing/routing_helpers.js`
- `data/signals/README.md`
- `data/signals/signal_helpers.js`
- `data/signals/signal_loader.js`
- `data/signals/signals_seed.json`
- `data/suppliers/supplier_loader.js`

### Utility/new files (new in backup)
- `download_crane_images.sh`
- `runs/overnight_domain_notes.md`

## Ignore / Generated (Do Not Merge as Source of Truth)
Treat as environment/run artifacts unless explicitly needed.

### Generated output data (new in backup)
- `companies_for_outreach.csv`
- `companies_priority_200.csv`
- `companies_priority_200_with_domains.csv`
- `data/monday_all_candidates.csv`
- `data/monday_all_email_candidates.csv`
- `data/monday_campaign_companies.csv`
- `data/monday_campaign_email_candidates.csv`
- `data/monday_campaign_plusvibes.csv`
- `data/monday_campaign_qa.json`
- `data/monday_campaign_verified.csv`
- `data/monday_companies_ranked.csv`
- `data/monday_company_domains.csv`
- `data/monday_company_qa.json`
- `data/monday_individual_candidates_qa.json`
- `data/monday_individual_email_candidates.csv`
- `data/monday_people_found.csv`
- `data/monday_people_pipeline_qa.json`
- `data/monday_plusvibes_combined.csv`
- `data/monday_plusvibes_individuals.csv`
- `data/monday_plusvibes_roles.csv`
- `data/monday_role_email_candidates.csv`
- `data/monday_top_250_companies.csv`
- `data/monday_top_500_companies.csv`
- `data/monday_verified_catchall.csv`
- `data/monday_verified_invalid.csv`
- `data/monday_verified_valid.csv`

### Learning bundle artifacts (new in backup)
- `docs_learning/build_log.md`
- `docs_learning/claude_browser_handoff.md`
- `docs_learning/learning_summary.md`
- `docs_learning/podcast_script.md`
- `docs_learning/upload_bundle/build_log.md`
- `docs_learning/upload_bundle/learning_summary.md`
- `docs_learning/upload_bundle/monday_all_email_candidates.csv`
- `docs_learning/upload_bundle/monday_company_domains.csv`
- `docs_learning/upload_bundle/monday_people_found.csv`
- `docs_learning/upload_bundle/monday_people_pipeline_qa.json`
- `docs_learning/upload_bundle/monday_plusvibes_combined.csv`
- `docs_learning/upload_bundle/monday_verified_catchall.csv`
- `docs_learning/upload_bundle/monday_verified_invalid.csv`
- `docs_learning/upload_bundle/monday_verified_valid.csv`
- `docs_learning/upload_bundle/podcast_script.md`
- `docs_learning/upload_bundle/qa_report.json`

## Only In Live
Keep these in live; they are local audit tooling artifacts from this reconciliation pass.
- `runs/repo_backup_audit.md`
- `scripts/audit_backup_repo.py`

## Recommended merge order
1. Merge all files under **Merge Now**.
2. Review and selectively merge **Review Manually**.
3. Do not merge **Ignore / Generated** unless explicitly required for archival/reporting.
