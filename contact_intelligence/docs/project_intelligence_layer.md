# Project Intelligence Layer (Phase 2 Backend)

## Scope in this pass
Built an additive Project Intelligence backend from currently available internal/runtime data.
No frontend presentation dependencies required.

## Schema added
Applied by:
- `contact_intelligence/schema/project_intelligence_extensions.sql`

Core tables:
- `signal_source_runs`
- `signal_events`
- `project_candidates`
- `project_signal_links`

Source-specific normalized tables:
- `permit_signal_items`
- `jobs_signal_items`
- `procurement_signal_items` (scaffold)
- `faa_notice_items` (scaffold)
- `company_news_signal_items` (scaffold)

## Source coverage in this pass
Implemented ingestion from:
- `data/opportunities/permits_imported.json`
- `data/jobs_imported.json`
- existing DB `signals` table

Scaffolded (schema ready, ingestion adapter later):
- procurement notices
- FAA notices
- company news

## Classification rules (deterministic)
Vertical buckets:
- data_centers
- power_energy
- industrial_manufacturing
- warehousing_logistics
- ports_terminals
- airports
- healthcare_hospitals
- semi_battery_ev
- rail_transit_bridge_infra
- institutional_campus_stadium
- other

Classification is keyword-based and explainable using project/company/signal context text.

## Scoring rules (heuristic)
For each project candidate:
- `estimated_spend_proxy` (vertical baseline + scale keywords + signal depth)
- `crane_relevance_score` (vertical baseline + lift/steel/rigging context)
- `mini_crane_fit_score` (tight-access/glazing/rooftop/interior/urban signals)
- `demand_score` (blend of spend + crane relevance)
- `timing_score` (recency + signal volume)
- `matchability_score` (state/source/company specificity)
- `confidence_score` (avg signal confidence + source/signal count depth)
- `monetization_score` (weighted blend for commercial usefulness)

`recommended_flag` and `recommendation_reason` are derived from monetization + confidence thresholds and mini-fit checks.

## Export outputs
Script:
- `contact_intelligence/scripts/export_project_intelligence.py`

Campaign-ready exports:
- `national_opportunity_candidates`
- `top_project_candidates`
- `top_data_center_candidates`
- `top_energy_candidates`
- `top_industrial_candidates`
- `top_logistics_candidates`
- `mini_opportunity_candidates`
- `recommended_expansion_candidates`

## Static JSON outputs
Generated under repo `data/static_exports/`:
- `project_candidates.json`
- `top_project_candidates.json`
- `mini_opportunity_candidates.json`
- `recommended_expansion_candidates.json`

If no records are present, JSON files are valid and may contain an empty array.

## Run commands
Build project intelligence:
```bash
python3 contact_intelligence/scripts/build_project_intelligence.py \
  --db ~/data_runtime/cranegenius_ci.db
```

Export campaign views + static JSON:
```bash
python3 contact_intelligence/scripts/export_project_intelligence.py \
  --db ~/data_runtime/cranegenius_ci.db \
  --output ~/data_runtime/exports \
  --json-output data
```

## Known limitations
- Current ingestion is intentionally narrow (permits + jobs + internal signals).
- Procurement/FAA/company-news adapters are scaffolded but not populated in this pass.
- Clustering is deterministic by normalized key (not full fuzzy entity resolution).
- Designed to favor precision and campaign usability over raw volume.
