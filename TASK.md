# Current Task

## Status: Inspect and fix real bugs — start with AI Planner

## Completed (Mar 7 2026)
- [x] 4 model-specific crane photos committed to assets/images/cranes/
- [x] marketplace/index.html: CRANE_IMG_MODEL implemented correctly
- [x] Context files committed to repo root
- [x] Bug audit: DC landing CTAs confirmed working (not broken)
- [x] Corrected directory paths in AGENT_CONTEXT.md

## Next: Inspect AI Planner (data-centers/ai-planner/index.html)

### Step 1 — Read before fixing
grep -n "fetch\|Authorization\|Bearer\|API_PROXY\|apiKey" data-centers/ai-planner/index.html | head -20

### Step 2 — Confirm the bug
If fetch call is missing Authorization header -> add it
If AI_PROXY_URL is empty or undefined -> that is the real issue

### Step 3 — Fix surgically via Python str.replace()

### Step 4
git add data-centers/ai-planner/index.html
git commit -m "fix: AI planner authorization header"
git push origin main

## After AI Planner
- Inspect lift-matrix filter pills (data-centers/lift-matrix/index.html)
- Unify design system across pages
