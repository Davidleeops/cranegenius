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

