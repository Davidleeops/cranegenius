# TASK.md — CraneGenius CI Active Task

## Status: DB REBUILT + ENRICHED ✅
Last updated: 2026-03-09

## Completed This Session
- [x] Apollo CSV imported (7,808 contacts, 6,287 companies)
- [x] DB rebuilt with location enrichment (city->state/province lookup)
- [x] Domain extracted from business emails (62% of companies)
- [x] Low-priority contacts flagged (conf=0.25, personal email + no title)
- [x] Dedupe run: 82 companies merged, 573 contacts merged
- [x] export_views.py confirmed working

## Current DB State (2026-03-09)
- Contacts total:    7,808
- Verified email:    5,402 (69%)
- Has phone:         7,558 (97%)
- Has title:         3,288 (42%)
- Has location:      3,994 (51%)
- High conf >=0.7:   2,032 (26%)
- Low priority:      1,765 (23%)
- Companies total:   6,287
- With domain:       3,886 (62%)
- With state:        2,968 (47%)

## Geography (contacts)
- Alberta (AB):      1,391 — oil patch, industrial, heavy lift
- BC (BC):           1,119 — Vancouver/Kelowna/Surrey
- Washington (WA):     359 — Seattle/Tacoma/Bellevue — Leavitt territory
- Oregon (OR):         149 — Portland — Leavitt territory

## GPT Readiness: ~88%
Gaps remaining:
1. Sector assignment (~5% tagged) — no SerpAPI run yet
2. LinkedIn (0%) — not in Apollo export
3. Title missing for 4,520 contacts (personal email, low priority)

## Immediate Next Tasks (Priority Order)
1. Run export_views.py and confirm all export files updated
2. Build GPT outreach prompt template using verified + high-conf contacts
3. SerpAPI domain enrichment on top 200 no-domain companies (10+ contacts)
4. Sector tagging pass using company name + domain keywords
5. Re-run pipeline for Chicago + NYC permit data
6. First retainer pitch to Erick / Leavitt Cranes (Pacific Northwest)

## Key File Locations
- DB:          ~/data_runtime/cranegenius_ci.db
- Exports:     ~/data_runtime/exports/
- Apollo CSV:  ~/Downloads/5d4729e4-27d9-42ae-9b7a-148d2641873d.csv
- Rebuild:     Run inline heredoc in terminal (see CHANGELOG_AGENT.md)
- contact_intelligence/: LOCAL ONLY — never push to GitHub

## Do NOT Touch
- src/pipeline.py
- contact_intelligence/scripts/normalize_records.py (broken — use heredoc rebuild)
