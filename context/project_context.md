# CraneGenius Project Context
# Injected into every LLM API call automatically. Edit to keep current.

## What CraneGenius Is
CraneGenius (cranegenius.com) is a crane rental intelligence and lead generation platform.
Scrapes municipal building permit data to find construction projects needing cranes.
Monetizes via retainer-based remote sales rep relationships with mid-market crane companies.

## Business Model
- Retainer: $1,500-$2,500/month per crane company client (positioned as remote sales rep)
- Deal brokering: Commission on closed transactions
- Enterprise CAA tiers: Priority Access ~$5K/mo, Embedded Partner ~$150K+/mo
- Transaction-first: Lead with single transaction before pitching subscription

## ICP — Ideal Customer
- Mid-market crane companies $1-5M revenue
- NOT large operators (have Dodge Reports already)
- Key markets: Chicago and Dallas relationships established

## Pipeline Stages
1. permit_ingestion — Pull from Chicago Socrata, Dallas, Phoenix, NYC APIs
2. normalization — Standardize across city formats
3. crane_scoring — Score permit for crane likelihood (type, cost, floors)
4. domain_resolution — Resolve contractor names to domains
5. contact_mining — Extract contacts from domains
6. email_generation — Generate email candidates
7. verification — MillionVerifier (seed_partial + enrichment_confident only)
8. sheets_export — Google Sheets via Apps Script

## Data Sources
- Chicago Socrata: data.cityofchicago.org/resource/ydr8-5enu.csv (best — live 2026 data)
- Dallas Socrata: outdated 2019 — Accela scraper in progress
- Phoenix: operational. NYC: operational.

## Current Status
- 700+ verified prospects in contact lists
- 200+ warm leads per pipeline run
- First retainer client: TARGET (not yet signed)

## Sales Positioning
- Reference exact permitted projects in every outreach email
- "I saw the permit for your 6-floor building at [address]" beats generic pitches

## Tech Stack
- GitHub Pages (Davidleeops/cranegenius), GoDaddy domain
- MillionVerifier API, Google Sheets/Apps Script, Formspree
- Anthropic API claude-sonnet-4-20250514
