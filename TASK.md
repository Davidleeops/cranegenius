# CraneGenius — Active Task

## Status: Jobs page fetch-based rebuild COMPLETE

### Completed
- Manpower page fetch-based rebuild committed fec9716
- Jobs page rewritten: fetches /data/static_exports/jobs_feed_items.json + job_contact_matches.json
- TASK.md and CHANGELOG updated

## Next Task: /opportunities/ page
- Fetch from data/static_exports/opportunity_feed_items.json (35 records)
- Merge opportunity_company_matches.json (165 records)
- Same pattern: loadData(), live banner, error state, no fabricated fallback

## Agent Boundaries
Claude lane: frontend HTML/CSS/JS only
Codex lane (locked): contact_intelligence/, src/ pipeline files
