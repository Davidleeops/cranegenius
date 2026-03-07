# Opportunities Data Foundation

This folder contains the first-pass Opportunity Intelligence data layer.

## Current files
- `opportunities.json`: primary repo opportunity dataset used by `/opportunities/` and `/opportunities/{slug}/`
- `seed_opportunities.json`: fallback seed dataset if imported paths are unavailable
- `opportunity_helpers.js`: category inference + unified opportunity lead payload builder

Dataset loader order is defined in `/opportunities/data_loader.js`:
1. imported dataset paths under `data/assets` / `data/opportunities/*import*`
2. `data/opportunities/opportunities.json`
3. `data/opportunities/seed_opportunities.json` fallback

## Future ingestion hooks (not implemented in this sprint)
Future scraper/agent pipelines can append or refresh `opportunities.json` from:
- building permits
- zoning approvals
- planning commission agendas
- environmental filings
- utility infrastructure projects
- data center development signals
- industrial facility permits
- public bid opportunities

## Notes
- Keep the opportunity object schema aligned with `config/opportunity_schema.json`.
- Keep lead object fields aligned with the platform unified lead model.
