# CraneGenius — Active Task

## HANDOFF TO CODEX — 2026-03-09
## Claude usage exhausted. Codex takes over for next 4 days.

---

## Completed by Claude (this session)
- manpower/index.html — fetch-based, commit fec9716 ✅
- jobs/index.html — fetch-based, commit f064568 ✅
- Both pages: loadData() fetches static_exports, live banner, error state, no fabricated data

---

## NEXT TASK FOR CODEX: /opportunities/ page

File: opportunities/index.html
Pattern: identical to manpower and jobs pages

Data sources:
- /data/static_exports/opportunity_feed_items.json (35 records)
  Fields to expect: id, title, company, location, description, permit_address,
  permit_type, estimated_value, start_date, source, daysAgo, matchCount
- /data/static_exports/opportunity_company_matches.json (165 records)
  Fields to expect: opportunity_id, company_name, contact_name, score, reason

UI requirements:
- loadData() async fetch of both files
- Merge matches onto opportunities by opportunity_id
- Stats bar: statOpps (count), statMatches (165), statCities (count unique cities)
- Green LIVE DATA banner with export timestamp on success
- Red error banner + error state card if fetch fails — zero fabricated fallback
- Filter: keyword search, city filter, permit type chips, match-only toggle
- Sort: newest, highest value, most matches
- Opportunity card: permit address as primary hook (that's the CraneGenius value prop)
- CTA on each card: "Get Crane Availability" → opens bot panel pre-loaded with permit context
- Post opportunity modal → Formspree mgoldjjb, moderation_status: pending_review
- AI bot hook: askAiAboutOpportunity(id) pre-loads permit address + estimated value into bot

Design system (match existing pages exactly):
- --gold:#c9a84c  --bg:#080e1a  --bg2:#0d1628  --bg3:#111e35
- Fonts: Bebas Neue (headings), DM Sans (body), DM Mono (labels/badges)
- Scripts: /config.js and /assets/js/page_ai_context.js

Formspree endpoint: mgoldjjb
Authority blocking regex: /(osha|asme|ansi|state board|licensing board|department of labor|federal|county|city of|commission|authority|regulator|regulatory)/i

---

## AFTER opportunities/ — remaining pages
- directory/index.html — same fetch pattern (contact_intelligence exports)
- acquisitions/index.html — Codex already modified, needs review + commit

## AGENT BOUNDARIES (PERMANENT)
Codex lane: contact_intelligence/, src/ pipeline files, schema, exports
Claude lane (when back): frontend HTML/CSS/JS only

## Repo
Local: ~/Downloads/cranegenius_repo/
Live: https://cranegenius.com
Deploy pattern: python3 inline heredoc, then git add/commit/push
