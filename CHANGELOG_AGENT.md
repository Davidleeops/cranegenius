# Agent Changelog

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

### Still broken — next agent starts here
- DC landing CTAs inert (Bug #1) — read TASK.md, fix data-centers/index.html
- AI Planner 401 (Bug #2)
- Lift Matrix filter pills (Bug #3-5)

### Do not revert
- marketplace/index.html CRANE_IMG_MODEL implementation is correct
- 4 model photos in assets/images/cranes/ are correct and committed

## 2026-03-07 — Bug audit + AGENT_CONTEXT corrected

### Findings
- DC landing CTAs (previously Bug #1) are FULLY WIRED — not broken
  routeToLiveBot() -> /?bot=1&msg= -> bootstrapBotFromQuery() -> openBotWithPrefill()
- ai-planner and lift-matrix are subdirectories under data-centers/, not root-level
- Corrected all paths in AGENT_CONTEXT.md

### Next agent
- Verify data-centers/ai-planner/index.html for real 401 bug before fixing
- Verify data-centers/lift-matrix/index.html filter pills before fixing
- Always read actual file first — bug list may be stale

## 2026-03-07 — AI Planner 401 fixed by Codex

### Done (Codex — commit ed93373)
- data-centers/ai-planner/index.html: both fetch calls to api.anthropic.com replaced with callAIProxy()
- Added AI_PROXY_URL = window.__CG_PROXY_URL__ constant
- Added callAIProxy(messages, maxTokens) helper function
- Added <script src="/config.js"></script>
- Verified: grep confirms zero direct api.anthropic.com calls remain

### Next agent — start here
Fix Lift Matrix filter pills in data-centers/lift-matrix/index.html
MUST grep to verify bug before writing any fix — see TASK.md for exact commands.
Also check Add to Plan button and Get Quotes/Request Full Analysis stubs in same file.

## 2026-03-07 — Lift Matrix lead capture + full bug audit complete

### Done
- Replaced openQuote() alert() stub with real Formspree lead capture modal
  Modal captures: name, email, project, equipment list. Source tagged lift-matrix.
- Full bug audit: DC CTAs, filter pills, addToPlan were never broken
- All bugs from original list resolved

### Site status
Primary funnel complete end-to-end. No blocking bugs remain.

### Next agent
No code tasks blocking revenue. Outreach is the priority.
If returning to code: unify design system. See TASK.md.

## 2026-03-07 — Security hardening pass

### Done
- Added `scripts/scan_secrets.sh` to detect common leaked key patterns before commit.
- Updated `.gitignore` with `.env.*`, `!.env.example`, and `*.bak`.
- Verified no active API keys are present in tracked web/source files.

### Remaining action
- Rotate provider keys if any were previously exposed in shell history or screenshots.

## 2026-03-07 — Outreach prep (safe messaging)

### Done
- Generated `data/dc_outreach_emails_safe.csv` using non-fabricated claims and neutral value framing.
- Generated `data/plusvibes_import_safe.csv` for direct campaign import.
- Preserved 204 verified contacts and deduplicated by email.

### Next step
- Launch outreach from the safe file and track bounce/reply rates by cohort.

## 2026-03-07 — Mobile funnel hardening

### Done
- `index.html`: added mobile bot breakpoint at 900px.
  - bot panel becomes full-width bottom sheet (`72vh`) on phones.
  - trigger switches from vertical side-tab to bottom-right pill CTA.
- `marketplace/index.html`: added mobile breakpoints at 1100px and 700px.
  - removes desktop `padding-right: 400px` on mobile.
  - bot panel becomes bottom sheet (`56vh`) with content preserved.
  - status bar/nav/hero/grid spacing optimized for narrow widths.

### Next step
- Run real-device QA (Safari iOS + Chrome Android):
  - open bot, submit message, tap Call CTA, submit Formspree quote.

## 2026-03-07 — Data Centers mobile conversion pass

### Done
- `data-centers/index.html`
  - Added 760px breakpoint for mobile nav and horizontal tab-strip behavior.
- `data-centers/ai-planner/index.html`
  - Added `subnav-strip` class for sticky tab row control on mobile.
  - Hid crowded center stepper on phones; kept primary quote CTA visible.
  - Made prompt footer stack cleanly on narrow screens.
- `data-centers/lift-matrix/index.html`
  - Fixed responsive selector typo (`nav,.filter-bar`), added `subnav-strip` class and mobile spacing.
  - Made floating summary panel full-width-friendly on phones.
  - Added tighter small-screen stats/nav behavior.

### Next step
- Real-device QA: iOS Safari + Android Chrome for all data-centers flows.

## 2026-03-07 — Mobile conversion polish (tap targets + CTA clarity)

### Done
- `index.html`
  - Mobile nav links, option cards, main buttons, and bot trigger aligned to 44px touch target baseline.
  - Improved mobile header subcopy readability.
- `marketplace/index.html`
  - Mobile nav links/buttons/inputs raised to 44px touch targets.
  - Tightened hero typography for above-fold clarity.
- `data-centers/index.html`
  - Mobile hero CTAs now full-width and centered.
  - Primary CTA/button classes and nav tabs adjusted to 44px touch targets.
- `data-centers/ai-planner/index.html`
  - Mobile CTAs/actions raised to 44px touch targets.
  - Improved hero readability and subnav touch sizing.
- `data-centers/lift-matrix/index.html`
  - Mobile filter/action controls raised to 44px touch targets.
  - Subnav link touch target sizing improved.

### Next step
- Device QA with conversion checklist (tap precision, above-fold CTA visibility, quote submit success).

## 2026-03-07 — Ecosystem architecture expansion (Phase 1)

### Done
- Created new page: `sell-your-company/index.html`
  - Positioning for succession/liquidity in crane + specialty lift ecosystem.
  - Primary CTA: Request a Confidential Discussion.
  - Secondary CTA: Join Demand Partner Network.
  - Intake fields include owner/company/location/equipment categories/revenue/EBITDA/timeline/contact method/notes.
  - Form routing via existing Formspree endpoint with acquisition-specific source + subject metadata.
  - Local browser event log (`cg_acq_leads`) for backward-compatible CRM handoff.
- Added Sell Your Company nav link to:
  - `index.html`
  - `marketplace/index.html`
  - `data-centers/index.html`
  - `data-centers/ai-planner/index.html` subnav strip
  - `data-centers/lift-matrix/index.html` subnav strip
- Added modular schema scaffold file:
  - `config/lift_ecosystem_schema.json`
- Added missing memory backlog file:
  - `FEATURE_BACKLOG.md`

### Notes
- Existing estimator/planner/marketplace flows were preserved unchanged.
- New architecture is layered non-destructively for backward compatibility.

## 2026-03-07 — Sell Your Company sprint-spec alignment

### Done
- Updated `sell-your-company/index.html` to match required section structure and conversion CTAs.
- Added required acquisition form fields including `phone` and strict required validation.
- Added separate lightweight demand partner form tagged distinctly from acquisition seller submissions.
- Kept existing Formspree integration pattern (non-breaking) for both forms.
- Added explicit local structured lead placeholders for future CRM bridge:
  - `acquisition_targets`
  - `acquisition_inquiries`
  - `demand_partner_inquiries`
- Updated schema scaffold in `config/lift_ecosystem_schema.json` with explicit inquiry entities.


---

## 2026-03-08 | Agent: Claude | Session: Operational Memory System

### Files Created
- docs_learning/lessons_learned.md
- docs_learning/experiments.md
- docs_learning/signal_intelligence_log.md
- docs_learning/system_architecture.md
- docs_learning/content_log.md
- runs/system_metrics_history.csv (headers only)
- scripts/append_metrics_history.py
- scripts/append_lesson.py
- scripts/append_experiment.py
- scripts/append_signal_log.py

### Files Updated
- TASK.md (appended completion note)
- CHANGELOG_AGENT.md (this entry)

### What Was NOT Touched
- src/ — untouched
- Pipeline logic — untouched
- Any existing files overwritten — none

### Next
ChatGPT assigns next task.

---

## 2026-03-08 | Agent: Claude | Session: Schema mismatch fix

### Decision
Aligned append_pipeline_metrics.py to the existing system_metrics_history.csv schema.
Both scripts now write identical column names. No new CSV created.

### Column mapping applied
- companies_processed -> companies
- domains_found -> valid_domains
- unresolved derived as (companies - valid_domains)
- people_found removed (not in committed schema)
- dataset arg added (default: "auto")

### Files changed
- scripts/append_pipeline_metrics.py (rewritten to match schema)
- TASK.md (appended)
- CHANGELOG_AGENT.md (this entry)

### Files not touched
- src/ — untouched
- system_metrics_history.csv — untouched
- append_metrics_history.py — untouched
