# Opportunities Data Foundation

This folder contains the first-pass Opportunity Intelligence data layer.

## Current files
- `opportunities.json`: structured opportunity records used by `/opportunities/` and `/opportunities/{slug}/`
- `opportunity_helpers.js`: category inference + unified opportunity lead payload builder

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
