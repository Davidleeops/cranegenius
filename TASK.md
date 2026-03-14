# CraneGenius — Active Task
## Next Task: 2026-03-13 — Claude (Frontend Agent)
## Status: READY TO BUILD

---

## COMPLETED THIS SESSION
- opportunities/index.html — FULL REPLACEMENT ✅
  - Commit: 9c84ba7 on main
  - Bebas Neue / DM Sans / DM Mono, correct design tokens
  - Live data fetch from /data/opportunities/opportunities.json + /data/opportunities_batch_2.json
  - De-dupe, authority filter, stats bar, 4-filter toolbar
  - "Get Crane Availability" bot CTA + Formspree modal (mgoldjjb) on every card

---

## ACTIVE TASK FOR CLAUDE: directory/index.html

### File to create
directory/index.html — NEW file (does not exist yet)

### Purpose
Company/contractor directory pulled from contact_intelligence exports.
Shows crane companies and GC contacts from the Apollo-enriched database.

### Data sources (check which exist in repo under /data/)
- Primary: /data/static_exports/directory_companies.json (if exported by Codex)
- Fallback: /data/static_exports/companies_export.json
- Check repo tree before building — use whatever Codex exported

### Design system (EXACT match — no deviations)
- Background: --bg:#080e1a (body), --bg2:#0d1628 (panels), --bg3:#111e35 (cards)
- Accent: --gold:#c9a84c
- Border: #1e3a5f
- Text: #e8f0ff (primary), #97a8c2 (muted)
- Fonts: Bebas Neue (headings), DM Sans (body), DM Mono (labels/badges)
- Google Fonts: Bebas+Neue|DM+Sans:wght@400;500;600|DM+Mono:wght@400;500

### Script requirements
- Load /config.js in <head> with defer
- Load /assets/js/page_ai_context.js before </body> if exists

### Functional requirements
- Fetch data on load, show LIVE DATA banner on success, error banner on failure
- Stats bar: total companies, states covered, verified contacts, avg confidence
- Filter: keyword (company name, state, city), state dropdown, sector dropdown
- Cards show: company_name, state/city, sector, domain, top contact name + title
- CTA on every card: "Get Crane Availability" — openBotWithPrefill() or /?bot=1&msg= fallback
- Secondary CTA: "View on Marketplace" → /marketplace/
- No fabricated fallback data — error card only if fetch fails

### Nav (match opportunities page)
Lift Estimator | Data Centers | Marketplace | Opportunities | Jobs | Directory | Sell

### Deploy pattern
cd ~/Downloads/cranegenius_repo
python3 - << 'PYEOF'
content = """..."""
import os; os.makedirs('directory', exist_ok=True)
with open('directory/index.html', 'w') as f: f.write(content)
PYEOF
git add directory/index.html
git commit -m "feat: directory page — company/contact fetch-based with bot CTA"
git push

---

## QUEUE AFTER THIS TASK
1. Review acquisitions/index.html — Codex modified, not confirmed committed
2. Verify /sell-your-company/ page — check if Formspree lead capture is wired
3. Global nav audit — add Directory to nav on all pages that are missing it

## AGENT BOUNDARIES (PERMANENT)
- Codex lane: src/, contact_intelligence/, data schemas, pipeline exports
- Claude lane: frontend HTML/CSS/JS only
- Do NOT cross lanes

## Repo
- Local: ~/Downloads/cranegenius_repo/
- Live: https://cranegenius.com
- Design ref: marketplace/index.html (correct Bebas Neue + DM Sans)
