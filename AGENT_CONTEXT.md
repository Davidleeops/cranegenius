# CraneGenius — Agent Context

## Project
CraneGenius (cranegenius.com) is a crane industry intelligence and lead-generation platform.
- Hosted: GitHub Pages, main branch root
- Repo: github.com/Davidleeops/cranegenius
- Local clone: ~/Downloads/cranegenius_repo
- All pages: single-file HTML/CSS/JS, no build step
- Deploy: git add / commit / push -> live in ~60 seconds

## Primary Funnel
Lift Estimator -> Crane Recommendation -> Get Crane Availability -> Lead Capture
NEVER introduce dead-end flows. Every CTA must go somewhere in the funnel.

## Actual Directory Structure
cranegenius_repo/
  index.html                        <- main site + chatbot
  marketplace/index.html            <- crane equipment marketplace
  data-centers/index.html           <- DC vertical landing page
  data-centers/ai-planner/index.html  <- AI Lift Planner
  data-centers/lift-matrix/index.html <- Lift Matrix (35 crane types)
  command-center/index.html         <- password-protected dashboard
  assets/images/cranes/             <- crane photo assets
  cloudflare/                       <- Cloudflare Worker proxy
  src/                              <- Python intelligence pipeline
  credentials/                      <- NEVER commit, in .gitignore

## Image Assets (committed Mar 7 2026)
Path: assets/images/cranes/
- model-liebherr-ltm.jpg — Liebherr LTM mobile crane — CC0
- model-grove-gmk.jpg — Grove GMK all-terrain — CC confirmed
- model-potain-tower.jpg — Potain tower crane — CC confirmed
- model-liebherr-lr.jpg — Liebherr crawler crane — CC confirmed
- spider-crane-01/02.jpg — CC BY-SA 3.0
- mini-crawler-01.jpg — CC BY-SA 2.5
- mobile-crane-01/02/03.jpg, indoor-crane-01.jpg — Unsplash

## Marketplace Listings & Photo Status
| ID | Company | Model | Photo |
|---|---|---|---|
| 1 | Apex Crane Chicago | Liebherr LTM 1100-5.2 | model-liebherr-ltm.jpg |
| 2 | Dallas Tower Co. | Potain MDT 389 | model-potain-tower.jpg |
| 3 | Southwest Heavy Lift | Manitowoc 18000 | gradient tile |
| 4 | Pacific NW Cranes | Grove GMK5250L | model-grove-gmk.jpg |
| 5 | Chicago Mini Lift | Jekko SPX532 | gradient tile |
| 6 | Midwest Glass Systems | Smartlift SL408 | gradient tile |
| 7 | Texas Boom Co. | Manitex 50128S | gradient tile |
| 8 | Phoenix Specialty | Liebherr LR 1300 | model-liebherr-lr.jpg |
| 9 | Windy City Crane | Terex RT 130 | gradient tile |
| 10 | Dallas Mini Lift | Jekko JF545 | gradient tile |

NEVER assign a wrong-model photo. Gradient tile is correct for unmatched listings.

## Marketplace Image System (current — do not revert)
- CRANE_IMG_MODEL keys match x.model exactly from listings array
- ig(type, model) returns {g, e, img} where img is null if no match
- Card uses photo background if img exists, gradient only if null
- Glyph opacity 0.25 when photo present, 1.0 when gradient tile

## Bug Status (verified Mar 7 2026)
VERIFIED WORKING — do not re-fix:
- data-centers/index.html CTAs: routeToLiveBot() is fully wired, bootstrapBotFromQuery() handles ?bot=1&msg= on homepage, openBotWithPrefill() defined and called. NOT broken.

REAL BUGS (unverified, needs inspection before fixing):
1. data-centers/ai-planner/index.html — suspected 401 on AI fetch (missing Authorization header)
2. data-centers/lift-matrix/index.html — filter pills activate visually but may not filter cards
3. data-centers/lift-matrix/index.html — Add to Plan button may not update plan panel
4. data-centers/lift-matrix/index.html — Get Quotes / Request Full Analysis may be alert() stubs

RULE: Always grep/sed the actual file to confirm a bug exists before writing any fix.

## Key People
- David Lee — Founder. Always use lemuel.lee.jr@gmail.com Chrome profile. NEVER use Ariel's profile.
- Erick Zampini — GM at Leavitt Cranes (Tacoma WA). NOT a separate partner. Potential commissioned remote salesman.
- Leavitt Cranes — Tacoma WA, Pacific NW / western Canada. NO Chicago presence.
- Great Lakes Lifting — Chicago area. Authorized Spydercrane (UNIC), Smartlift, Wood's Powr-Grip dealer.
- Ariel — David's fiancee. Never use her Chrome profile.

## Design System
- Marketplace: Bebas Neue + DM Sans + DM Mono; --gold #c9a84c; --bg #080e1a
- DC Landing / Lift Matrix: Barlow Condensed; navy #0a0e1a
- AI Planner: Syne; #03060f
- TODO: unify to single design system

## Tools & Integrations
- AI: claude-sonnet-4-20250514 via Anthropic API (routed through Cloudflare Worker proxy)
- Lead capture: Formspree formspree.io/f/mgoldjjb
- CRM: Google Sheets via Apps Script
- Email verification: MillionVerifier
- Outreach: PlusVibes

## Critical Dev Rules
- NEVER use TextEdit to save HTML — corrupts to HTML4
- Use Python file writes or VS Code for all HTML edits
- Surgical edits only — str_replace over full file rebuilds
- Sensitive files never in repo: data/, credentials/, *.csv in .gitignore
- GitHub Pages: subdirectory + index.html pattern only
- After Python patch: always check git status shows modified before committing
- ALWAYS grep/read the actual file before writing any fix — bug list may be stale
