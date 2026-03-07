# Current Task

## Status: Ready — fix DC landing page CTAs

## Completed (Mar 7 2026)
- [x] 4 CC-licensed model-specific crane photos downloaded and committed
- [x] marketplace/index.html patched with CRANE_IMG_MODEL (model-keyed, not type-keyed)
- [x] ig() updated to accept model param, returns null for unmatched models
- [x] Confirmed live on GitHub

## Next: Fix DC Landing Page CTAs (Bug #1)

### File
data-centers/index.html

### Problem
Two CTA buttons are completely inert — no href, no onclick:
- GET MY LIFT PLAN
- TALK TO AN EXPERT

### Fix
- GET MY LIFT PLAN -> onclick window.location.href='/lift-estimator/index.html'
- TALK TO AN EXPERT -> onclick window.location.href='/marketplace/index.html'
- No dead ends. Both must enter the funnel.

### Steps
1. grep for button text in data-centers/index.html to find exact HTML
2. Add onclick handlers via Python str.replace()
3. Verify git status shows modified
4. git add data-centers/index.html
5. git commit -m "fix: wire DC landing CTAs to funnel"
6. git push origin main

## After DC CTAs
- Bug #2: AI Planner 401 — add Authorization header
- Bug #3: Lift Matrix filter pills — add data-build, data-phase, data-category to cards
- Bug #4: Lift Matrix Add to Plan
- Bug #5: Lift Matrix alert() stubs
- Unify design system
