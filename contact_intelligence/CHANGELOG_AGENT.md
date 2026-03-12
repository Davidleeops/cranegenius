# CHANGELOG — Agent Handoff Log

---

## 2025-03-09 — Claude (claude-sonnet-4-20250514) — v2 Full Build

**Context:** Prior session built a simpler v1 (cranegenius_contact_system/). This session
builds the full v2 spec: three-layer architecture with CRM, reference/memory, and
learning/evaluation layers.

**Implementation path chosen:** Path B — new isolated module at `contact_intelligence/`

**Files created:**
```
contact_intelligence/
  AGENT_CONTEXT.md
  TASK.md
  CHANGELOG_AGENT.md
  requirements.txt
  schema/
    create_tables.sql     (20+ tables, 3 layers)
    indexes.sql           (40+ indexes)
    seed_sectors.sql      (20 crane-demand sectors)
  scripts/
    init_db.py            — init DB + runtime folders
    import_csv.py         — contact CSVs → source_records
    import_seed_data.py   — seed CSVs → canonical + domain_evidence + patterns
    normalize_records.py  — source_records → companies + contacts
    dedupe_people_companies.py — merge duplicates
    score_domain_evidence.py   — score domain candidates from feedback+evidence
    update_contact_patterns.py — infer email patterns from verified contacts
    log_feedback.py            — log bounce/reply/verified outcomes
    run_gold_truth_checks.py   — benchmark accuracy vs gold set
    export_views.py            — generate 7 .xlsx exports
    search_cli.py              — CLI search (15+ filters)
    backup_db.py               — timestamped backup
  docs/
    README.md
    integration_notes.md
  config/
    field_mappings.example.json
  sample_data/
    sample_contacts.csv
    sample_seed.csv
```

**Key design decisions:**
- `contacts` table replaces old `people` table — consolidated with role, seniority, confidence
- All 20 crane-demand sectors seeded at init
- `domain_evidence` supports multiple domain candidates per company with scores
- `contact_patterns` inferred from verified emails, updated by feedback
- `score_domain_evidence.py` uses feedback outcomes to adjust scores
- `run_gold_truth_checks.py` supports `--promote` to push verified data to canonical layer
- Export includes `top100_by_sector.xlsx` with one tab per active sector

**Smoke test results:** Full end-to-end pipeline ran successfully on sample data.
See TASK.md for exact commands and verified outputs.

**Did NOT touch:** src/pipeline.py or any live pipeline code.

**Next steps for next agent:**
1. Add `target_score` computation to companies based on signals + project + sector weights
2. Wire permit pipeline to auto-import to CI system via a wrapper script
3. Build `assign_top_targets.py` to populate top_target_lists + top_target_entries
4. Add `--sector` filter to import_csv.py to auto-assign sector on import
5. Build Streamlit mini-dashboard on top of search_cli.py queries
