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
