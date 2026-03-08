# Current Task

## Status: Core funnel complete — ready for outreach

## Completed (Mar 7 2026)
- [x] Marketplace: CRANE_IMG_MODEL model-specific photos implemented
- [x] AI Planner 401: fixed by Codex (commit ed93373) — routed through Cloudflare proxy
- [x] Lift Matrix openQuote(): replaced alert() stub with real Formspree lead capture modal
- [x] Bug audit complete: DC CTAs, filter pills, addToPlan all confirmed working
- [x] Context files committed and accurate

## Site Status
All known bugs resolved. Primary funnel functional end-to-end:
Lift Matrix -> Add to Plan -> REQUEST QUOTE -> modal captures lead -> Formspree

## Next Priority: OUTREACH (no code needed to start earning)
1. Launch PlusVibes campaign to 367 verified DC contacts
2. Sign first crane company retainer at $2,500/mo (Great Lakes Lifting or Leavitt via Erick)
3. Verify electrical contractors list via MillionVerifier before sending

## Next Code Task (lower priority than outreach)
Unify design system — 3 different font/color systems exist:
- Marketplace: Bebas Neue + gold #c9a84c
- DC Landing/Lift Matrix: Barlow Condensed + navy #0a0e1a
- AI Planner: Syne + #03060f
Consolidate to one.

## Security Update (2026-03-07)
- [x] Ran repo secret scan on tracked source files.
- [x] Added `scripts/scan_secrets.sh` for repeatable local scanning.
- [x] Hardened `.gitignore` for `.env.*` and backup files (`*.bak`).
- [ ] Rotate any API keys that were ever exposed in terminal/history snapshots.

## Outreach Prep Update (2026-03-07)
- [x] Built safe outreach file: `data/dc_outreach_emails_safe.csv` (204 rows)
- [x] Built PlusVibes import file: `data/plusvibes_import_safe.csv` (204 rows)
- [x] Removed fabricated claim style from new generated messaging
- [ ] Launch campaign + monitor reply/bounce rates in first 24h

## Mobile Funnel Update (2026-03-07)
- [x] Homepage bot UI now adapts for phones (bottom pill trigger + full-width bottom sheet).
- [x] Marketplace now drops 400px desktop sidebar offset on mobile and uses a bottom chat sheet.
- [x] Marketplace nav/cards/hero tightened for smaller screens.
- [ ] Manual device QA on iPhone + Android before Monday outreach.

## Data Centers Mobile Pass (2026-03-07)
- [x] `data-centers/index.html` nav + tab-strip improved for phones.
- [x] `data-centers/ai-planner/index.html` mobile nav simplified; sticky subnav spacing fixed.
- [x] `data-centers/lift-matrix/index.html` responsive typo fixed + mobile summary panel layout improved.
- [ ] Run manual end-to-end QA on real devices before Monday send.

## Mobile Conversion Polish (2026-03-07)
- [x] Raised key mobile tap targets to ~44px across core CTA surfaces.
- [x] Improved above-the-fold CTA clarity on Data Centers landing.
- [x] Increased small-screen readability for hero/subcopy on key funnel pages.
- [ ] Final real-device QA pass before Monday campaign send.

## Ecosystem Expansion Phase 1 (2026-03-07)
- [x] Added `/sell-your-company/` page with confidential acquisition intake form.
- [x] Wired non-breaking navigation links to Sell Your Company from core public pages.
- [x] Added modular entity schema scaffold: `config/lift_ecosystem_schema.json`.
- [x] Added `FEATURE_BACKLOG.md` to support persistent agent memory workflow.
- [ ] Phase 2: expand marketplace categories + lift-planner category pathways.

## Sell Page Sprint Alignment (2026-03-07)
- [x] Hero and content sections updated to exact sprint wording and structure.
- [x] Primary acquisition form includes required `phone` and full required field set.
- [x] Secondary demand-partner CTA implemented as separate lightweight form.
- [x] Internal data distinction implemented: `acquisition_targets`, `acquisition_inquiries`, `demand_partner_inquiries`.



---

## Completed by Claude — 2026-03-08

- Created docs_learning/lessons_learned.md
- Created docs_learning/experiments.md
- Created docs_learning/signal_intelligence_log.md
- Created docs_learning/system_architecture.md
- Created docs_learning/content_log.md
- Created runs/system_metrics_history.csv
- Created scripts/append_metrics_history.py
- Created scripts/append_lesson.py
- Created scripts/append_experiment.py
- Created scripts/append_signal_log.py
- Updated TASK.md and CHANGELOG_AGENT.md

Next: ChatGPT assigns next task. No src/ changes made.

---

## Completed by Claude — 2026-03-08

Resolved schema mismatch in scripts/append_pipeline_metrics.py.
Aligned its output columns to match system_metrics_history.csv and append_metrics_history.py.
Column mapping: companies_processed->companies, domains_found->valid_domains,
unresolved derived from (companies - valid_domains), people_found removed (not in schema).
Both scripts now write compatible rows to the same CSV.
