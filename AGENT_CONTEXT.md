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

## Active Pages
| Page | Path |
|---|---|
| Main site | index.html |
| Marketplace | marketplace/index.html |
| Data Centers landing | data-centers/index.html |
| AI Lift Planner | ai-planner/index.html |
| Lift Matrix | lift-matrix/index.html |
| Lift Cost Estimator | lift-estimator/index.html |
| Command Center (pw protected) | command-center/index.html |

## Image Assets (committed Mar 7 2026)
Path: assets/images/cranes/
- model-liebherr-ltm.jpg — Liebherr LTM mobile crane — CC0
- model-grove-gmk.jpg — Grove GMK all-terrain — CC confirmed
- model-potain-tower.jpg — Potain tower crane — CC confirmed
- model-liebherr-lr.jpg — Liebherr crawler crane — CC confirmed
- spider-crane-01.jpg, spider-crane-02.jpg — CC BY-SA 3.0
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

## Known Bugs (priority order)
1. DC landing CTAs inert — GET MY LIFT PLAN and TALK TO AN EXPERT have no onclick/href in data-centers/index.html
2. AI Planner 401 — missing Authorization: Bearer header in ai-planner/index.html
3. Lift Matrix filter pills — pills activate visually but dont filter cards (missing data attributes on cards)
4. Lift Matrix Add to Plan — button doesnt update plan panel
5. Lift Matrix alert() stubs — Get Quotes and Request Full Analysis show placeholder alert()

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
- AI: claude-sonnet-4-20250514 via Anthropic API
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
