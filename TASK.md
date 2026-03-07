# Current Task

## Status: Ready — inspect and fix Lift Matrix filter pills

## Completed (Mar 7 2026)
- [x] 4 model-specific crane photos committed to assets/images/cranes/
- [x] marketplace/index.html: CRANE_IMG_MODEL implemented correctly
- [x] Context files committed to repo root (AGENT_CONTEXT.md, TASK.md, CHANGELOG_AGENT.md)
- [x] Bug audit: DC landing CTAs confirmed working (not broken)
- [x] Bug audit: Corrected directory paths in AGENT_CONTEXT.md
- [x] AI Planner 401 fixed (commit ed93373) — both fetch calls now route through Cloudflare Worker proxy via callAIProxy()

## Next: Inspect Lift Matrix Filter Pills (Bug #2)

### File
data-centers/lift-matrix/index.html

### Step 1 — Verify bug exists before touching anything
grep -n "data-build\|data-phase\|data-category\|filterPill\|filter" data-centers/lift-matrix/index.html | head -20

### Step 2 — Check card HTML for missing data attributes
grep -n "class=.*card\|data-build\|data-phase\|data-category" data-centers/lift-matrix/index.html | head -20

### Step 3 — If confirmed broken
Add missing data-build, data-phase, data-category attributes to each card element.
Fix filter pill onclick handlers to actually filter by those attributes.
Use Python str.replace() surgical patch only.

### Step 4
git add data-centers/lift-matrix/index.html
git commit -m "fix: lift matrix filter pills now filter cards correctly"
git push origin main

### Step 5 — Also check in same file
- Add to Plan button: does it update the plan panel?
- Get Quotes / Request Full Analysis: are these alert() stubs or real handlers?

## After Lift Matrix
- Unify design system across all pages (Barlow Condensed + navy #0a0e1a as base)
- Add real crane company listings to marketplace when first retainer client signs
