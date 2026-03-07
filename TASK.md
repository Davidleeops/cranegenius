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

