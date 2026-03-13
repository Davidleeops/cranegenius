# CraneGenius — Active Task

## Controller Audit: 2026-03-13 — Claude (Controller Agent)
## Status: CORRECTION TASK ISSUED

---

## AUDIT FINDINGS

### opportunities/index.html — PROTOCOL VIOLATION
File exists at opportunities/index.html but Codex built the WRONG version.

Violations found (confirmed by live file inspection):
- WRONG font: Barlow Condensed (must be Bebas Neue for headings, DM Sans body, DM Mono labels)
- WRONG colors: missing --gold:#c9a84c, missing --bg:#080e1a
- WRONG data path: fetches /data/opportunities/opportunities.json via external data_loader.js
  The spec required direct inline fetch — no external loader dependency
- MISSING: "Get Crane Availability" CTA on cards
- MISSING: openBotWithPrefill() bot integration
- MISSING: Formspree lead capture modal (endpoint: mgoldjjb)
- MISSING: LIVE DATA green banner / error state red banner
- MISSING: project_address as primary card hook (permit_address equivalent)
- MISSING: /config.js script load

### Data path clarification (confirmed by tree inspection):
- /data/static_exports/ does NOT exist in the repo
- /data/opportunities/opportunities.json DOES exist with correct schema
- Use /data/opportunities/opportunities.json as the data source
- Also fetch /data/opportunities_batch_2.json for batch 2 records

### Confirmed correct files (do NOT touch):
- jobs/index.html — EXISTS, correct, fetch-based with static_exports reference
- manpower/index.html — EXISTS, correct

---

## ACTIVE TASK FOR CODEX: Replace opportunities/index.html

### File to replace
opportunities/index.html — FULL REPLACEMENT, not a patch

### Data sources (both exist in repo)
- PRIMARY: /data/opportunities/opportunities.json
  Schema: { "version": "v0.1", "opportunities": [ { opportunity_id, project_name,
  project_slug, project_address, city, state, country, project_type, industry_segment,
  developer_optional, general_contractor_optional, project_stage,
  estimated_start_date_optional, lift_requirements_summary,
  recommended_lift_categories, ... } ] }
- SECONDARY: /data/opportunities_batch_2.json (same schema, merge by opportunity_id)
- De-dupe merged array by opportunity_id before render

### Design system (EXACT match required — no deviations)
- Background: --bg:#080e1a (body), --bg2:#0d1628 (panels), --bg3:#111e35 (cards)
- Accent: --gold:#c9a84c
- Border: #1e3a5f
- Text: #e8f0ff (primary), #97a8c2 (muted)
- Fonts: Bebas Neue (all headings h1-h3), DM Sans (body text), DM Mono (labels/badges/stats)
- Google Fonts import: Bebas+Neue|DM+Sans:wght@400;500;600|DM+Mono:wght@400;500

### Script requirements
- Load /config.js in <head> with defer attribute
- Load /assets/js/page_ai_context.js before closing </body> if it exists

### Functional requirements

#### Data loading
- async loadData() fetches both JSON files concurrently (Promise.allSettled)
- Merges results, de-dupes by opportunity_id
- On success: show green LIVE DATA banner with record count
- On ALL fetch failure: show red error banner, render ONE error state card
  "Pipeline data unavailable. Check /data/opportunities/opportunities.json"
- ZERO fabricated fallback data. If data missing, error card only.

#### Stats bar (above grid)
- statOpps: total opportunity count
- statCities: count of unique city values
- statTypes: count of unique project_type values
- statStage: count of "permit_filed" or "active" stage records

#### Filters
- Text input: keyword search across project_name, project_address, city, project_type
- Dropdown: filter by state (populated from data)
- Dropdown: filter by project_type (populated from data)
- Dropdown: filter by project_stage (populated from data)
- Filters apply on input/change events, update count display

#### Sort
- Select: Newest (estimated_start_date_optional desc), A-Z (project_name), Stage

#### Cards (grid, 3 cols desktop / 2 tablet / 1 mobile)
Primary hook on every card (in order of priority):
1. project_address + city, state — displayed prominently at top
2. project_name as secondary title
3. project_type badge (DM Mono, gold border)
4. project_stage badge (DM Mono)
5. lift_requirements_summary (2-3 lines)
6. recommended_lift_categories as tag chips (max 3)

#### CTAs on every card (required — no exceptions)
Button 1 (primary, gold bg): "Get Crane Availability"
  Action: if window.openBotWithPrefill exists, call:
    window.openBotWithPrefill("Crane needed: " + project_name + " at " + project_address + ", " + city + " " + state + " | Type: " + project_type)
  Else: window.location.href = "/?bot=1&msg=" + encodeURIComponent(same string)
  Never dead-end.

Button 2 (secondary): "View on Marketplace"
  href="/marketplace/?type=" + encodeURIComponent(recommended_lift_categories[0] || 'mobile_cranes')

#### Post opportunity modal
- Trigger: "Post an Opportunity" button in page header
- Form fields: project_name, project_address, city, state, project_type, contact_email
- Hidden field: moderation_status = "pending_review"
- Submits to: https://formspree.io/f/mgoldjjb (POST, JSON)
- On success: show confirmation message inside modal, close after 2s
- On error: show inline error, keep modal open

#### Authority blocking
Filter out from display any record where project_name OR project_address matches:
/(osha|asme|ansi|state board|licensing board|department of labor|federal|county|city of|commission|authority|regulator|regulatory)/i

### Deploy pattern (EXACT — no downloads, no TextEdit)
cd ~/Downloads/cranegenius_repo
python3 - << 'PYEOF'
content = """..."""  # full HTML here
with open('opportunities/index.html', 'w') as f:
    f.write(content)
PYEOF
git add opportunities/index.html
git commit -m "fix: opportunities page — correct design system, bot CTA, Formspree, live data"
git push

---

## AGENT BOUNDARIES (PERMANENT)
- Codex lane: src/, contact_intelligence/, data schemas, pipeline exports
- Claude lane (when back): frontend HTML/CSS/JS only
- Do NOT cross lanes under any circumstances

## QUEUE AFTER THIS TASK
1. directory/index.html — new fetch-based page (contact_intelligence exports)
2. Review acquisitions/index.html — Codex modified but not confirmed committed

## Repo
- Local: ~/Downloads/cranegenius_repo/
- Live: https://cranegenius.com
- Design ref: marketplace/index.html (correct Bebas Neue + DM Sans implementation)
