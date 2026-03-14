

---

## 2026-03-13 — opportunities/index.html Full Replacement
Agent: Claude (Sonnet 4.6)

### What was done
- Full replacement of opportunities/index.html — TASK.md correction task executed
- Previous file used Barlow Condensed, wrong colors, external data_loader.js dependency, no bot CTA
- New file: Bebas Neue / DM Sans / DM Mono font stack, --bg:#080e1a / --gold:#c9a84c design tokens
- Dual fetch from /data/opportunities/opportunities.json + /data/opportunities_batch_2.json (Promise.allSettled)
- De-dupe by opportunity_id, authority regex filter
- Stats bar: opps count, cities, types, permit_filed/active count
- 4-filter toolbar: keyword search, state, project_type, project_stage
- LIVE DATA green banner on success, red error banner on all-fetch failure
- "Get Crane Availability" gold button on every card — openBotWithPrefill() with /?bot=1&msg= fallback
- "View Marketplace" secondary button links to /marketplace/?type=
- "Post Opportunity" modal — Formspree endpoint mgoldjjb, pending_review hidden field
- Committed to main as 9c84ba7

### Files Modified
- opportunities/index.html — full replacement

### Verified Correct (do not touch)
- jobs/index.html, manpower/index.html — confirmed correct, fetch-based
- data/opportunities/opportunities.json — 12 records, schema confirmed

### Next Step for Next Agent
Read TASK.md — active task is now: directory/index.html (new fetch-based page from contact_intelligence exports)
After that: review acquisitions/index.html (Codex modified, not confirmed committed)# Agent Changelog


---
Timestamp: 2026-03-07
Agent: GPT
Files Modified:
- AGENT_CONTEXT.md
- TASK.md
- CHANGELOG_AGENT.md
Summary:
Created the repository memory files in the repo root so future AI agents can share project context safely.
Reason:
The memory files were missing from the repo root, which risked losing project state and causing future regressions.
Next Step:
Commit these files to the repository and verify marketplace image integrity.
---


## 2026-03-07 — Context files expanded + GPT review fixes applied


### Done
- Expanded AGENT_CONTEXT.md with full project state (listings table, image system, bugs, dev rules)
- Updated TASK.md with explicit /index.html CTA paths per GPT feedback
- CHANGELOG now uses append mode (not overwrite) per GPT feedback
- commit uses --allow-empty per GPT feedback
---
Timestamp: 2026-03-09
Agent: Claude (Sonnet)
Files Modified: TASK.md, CHANGELOG_AGENT.md
Summary:
Apollo CSV (7,808 contacts) imported and DB fully rebuilt with enrichment.

## What Was Done
1. Apollo export (5d4729e4 UUID file) loaded — 7,808 named contacts, 5,402 verified emails
2. DB rebuilt from scratch via inline heredoc (not a saved file — run from terminal each time)
3. City->state/province lookup applied: 3,994 contacts now have location (51%)
4. Domain extracted from business emails: 3,886 companies now have domain (62%)
5. Low-priority contacts flagged conf=0.25 (personal email + no title): 1,765 contacts
6. Dedupe run: 82 companies merged, 573 contacts merged on exact_phone signal
7. TASK.md updated and committed to GitHub

## Key Technical Facts for Next Agent
- DB: ~/data_runtime/cranegenius_ci.db
- Apollo CSV: ~/Downloads/5d4729e4-27d9-42ae-9b7a-148d2641873d.csv
- No rebuild_db.py file exists — rebuild is run as inline heredoc (see session transcript)
- To rebuild: run the python3 - << 'HEREDOC' ... HEREDOC block from terminal
- contact_intelligence/ is LOCAL ONLY — never push to GitHub
- normalize_records.py is still broken — do NOT use it
- export_views.py works correctly: run from cranegenius_repo/

## Current DB Numbers
- Contacts: 7,808 | Verified: 5,402 (69%) | Phone: 7,558 (97%)
- High conf >=0.7: 2,032 | Low priority: 1,765
- Companies: 6,287 | With domain: 3,886 (62%) | With state: 2,968 (47%)

## Geography (contacts)
- AB: 1,391 | BC: 1,119 | WA: 359 | OR: 149 — heavy Canadian Prairie + PNW

## GPT Readiness: ~88%
Remaining gaps: sector assignment (~5%), LinkedIn (0%), title missing (58%)

## Next Priority Tasks
1. Run export_views.py to confirm latest exports
2. Build GPT outreach prompt template (high-conf verified contacts)
3. SerpAPI domain enrichment on top 200 no-domain companies
4. Sector tagging pass
5. First retainer pitch to Erick / Leavitt Cranes (WA/OR territory)


---
## 2026-03-09 — Claude (Session: Manpower + Jobs fetch rebuild)

### Completed
- Manpower page: fetch-based from static_exports, commit fec9716
- Jobs page: fetch-based from /data/static_exports/jobs_feed_items.json + job_contact_matches.json
- Stats bar, live banner, error state, apply/post modals, AI bot hooks — all wired
- TASK.md and CHANGELOG updated via inline Python (no download scripts)

### Deploy method
- Mac terminal inline Python heredoc — zero file downloads

### Next agent
- /opportunities/ page — same fetch pattern
- Do NOT touch Codex-lane files (src/, contact_intelligence/)

---
## 2026-03-09 — Claude (Session: Manpower + Jobs fetch rebuild)

### Completed
- Manpower page: fetch-based from static_exports, commit fec9716
- Jobs page: fetch-based from /data/static_exports/jobs_feed_items.json + job_contact_matches.json
- Stats bar, live banner, error state, apply/post modals, AI bot hooks wired
- TASK.md and CHANGELOG updated via inline Python

### Next agent
- /opportunities/ page same fetch pattern
- Do NOT touch Codex-lane files (src/, contact_intelligence/)


---

## 2026-03-13 — Controller Audit + TASK.md Correction
Agent: Claude (Controller)

### What was audited
Read AGENT_CONTEXT.md, TASK.md, and repo file tree. Inspected live files:
- opportunities/index.html — live content inspection via raw.githubusercontent.com
- data/opportunities/opportunities.json — schema confirmed
- jobs/index.html — confirmed EXISTS and correct (GitHub blob tab was stale cache)
- manpower/index.html — previously confirmed correct

### What was found
opportunities/index.html is a PROTOCOL VIOLATION. Codex built the wrong version:
- Wrong font: Barlow Condensed (must be Bebas Neue / DM Sans / DM Mono)
- Wrong colors: no --gold:#c9a84c, no --bg:#080e1a
- Wrong data path: fetches /data/opportunities/ via external data_loader.js
- Missing: "Get Crane Availability" CTA on cards
- Missing: openBotWithPrefill() bot integration
- Missing: Formspree modal (mgoldjjb)
- Missing: LIVE DATA / error banners
- Missing: project_address as primary card hook
- Missing: /config.js script load

### Data path clarification confirmed
- /data/static_exports/ does NOT exist in repo (prior TASK.md spec was wrong)
- Use /data/opportunities/opportunities.json (EXISTS, correct schema)
- Also merge /data/opportunities_batch_2.json

### What was changed
- TASK.md: full replacement with corrected audit findings + precise Codex task spec
  - Exact design tokens, font stack, data paths, CTA requirements, Formspree spec
  - Bot integration: openBotWithPrefill() with fallback to /?bot=1&msg=
  - Deploy pattern, agent boundary reminder, queue after this task

### Files Modified
- TASK.md — replaced with correction task for opportunities/index.html

### Next Step for Codex
Read TASK.md and execute: replace opportunities/index.html with correct version.
Commit: "fix: opportunities page — correct design system, bot CTA, Formspree, live data"

### Verified Correct (do not touch)
- jobs/index.html — EXISTS, fetch-based
- manpower/index.html — EXISTS, fetch-based
- data/opportunities/opportunities.json — schema confirmed correct
