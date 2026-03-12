# AGENT_WORK_BOUNDARIES.md

## Primary Lane (Current)
Phase 2 backend normalization and matching in `contact_intelligence/`.

## In Scope
- `contact_intelligence/schema/*`
- `contact_intelligence/scripts/*` (normalization/matching/export/compat)
- feed-backed matching outputs and related export views

## Out of Scope
- `src/monday_people_pipeline.py`
- `src/people_*` pipeline behavior
- static site UI/UX changes not directly needed for backend matching

## Temporary Edit Lock (for parallel-agent safety)
Avoid concurrent edits by other agents to these files while Phase 2 is stabilizing:
- `contact_intelligence/schema/create_tables.sql`
- `contact_intelligence/schema/indexes.sql`
- `contact_intelligence/scripts/export_views.py`
- `contact_intelligence/scripts/dedupe_people_companies.py`
- `contact_intelligence/scripts/normalize_and_match_feeds.py`
- `contact_intelligence/scripts/export_static_json.py`
- `contact_intelligence/docs/static_json_exports.md`

- `contact_intelligence/schema/project_intelligence_extensions.sql`

- `contact_intelligence/scripts/build_project_intelligence.py`

- `contact_intelligence/scripts/export_project_intelligence.py`

- `contact_intelligence/docs/project_intelligence_layer.md`

Release lock after Phase 2 validation run + export verification.
