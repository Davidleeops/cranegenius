# CraneGenius Contact Intelligence — Agent Context

## Folder
`contact_intelligence/` inside ~/Downloads/cranegenius_repo/

## Architecture: Three Layers
1. **Core CRM/Marketplace** — sectors, companies, contacts, projects, crane_requirements, signals, outreach_history, opportunities, deal_pipeline, equipment_fleet, operator_network
2. **Reference/Memory** — canonical_companies, company_aliases, domain_evidence, contact_patterns, source_registry
3. **Learning/Eval** — feedback_outcomes, gold_truth_companies, gold_truth_contacts

## Database
`~/data_runtime/cranegenius_ci.db` — OUTSIDE the repo. Never commit it.
Override: `CRANEGENIUS_CI_DB` env var

## What NOT to Touch
- `src/pipeline.py` and any live pipeline logic — untouched
- `data/` folder with production CSVs — read-only input only

## Key Script Sequence
```
init_db → import_csv / import_seed_data → normalize_records →
dedupe_people_companies → score_domain_evidence → update_contact_patterns →
export_views → search_cli / log_feedback → run_gold_truth_checks → backup_db
```

## Sectors (20)
Data Center Construction, Semiconductor Manufacturing, Utility Grid Infrastructure,
LNG and Natural Gas Infrastructure, Wind Energy Projects, Battery and EV Manufacturing,
Industrial Shutdown and Turnaround, Nuclear Plant Refurbishment, Solar Farm Construction,
Port and Maritime Infrastructure, Bridge and Civil Infrastructure, Highway Infrastructure,
Airport Construction and Expansion, Modular and Prefabricated Construction,
Petrochemical Facilities, Steel and Heavy Manufacturing, Water Treatment Infrastructure,
LNG Storage Tank Construction, Mining Infrastructure, Disaster Recovery Infrastructure

## Business Lines
cranegenius | real_estate | consulting | recruiting | general_bd

## Key Relationships
sector → company → contacts/projects → crane_requirements → opportunities
canonical_companies ← domain_evidence (scored, multi-candidate)
contacts ← contact_patterns (inferred from verified emails)
feedback_outcomes → re-scores domain_evidence + contact_patterns
gold_truth → benchmark accuracy
