# Outbound Generation Summary

- Total usable outbound rows: **6**
- Likely spider/mini rows: **0**
- Broader opportunity rows: **6**

## Files Created
- `data/outbound/outbound_likely_spider_mini.csv`
- `data/outbound/outbound_broader_projects.csv`

## Primary Bottlenecks
- `excluded_duplicate_email_global`: 17731
- `source:internal:nyc_call_list.csv:excluded_blank_email`: 1502
- `source:internal:contact_conflicts.csv:excluded_blank_email`: 289
- `source:internal:dc_outreach_emails.csv:excluded_blank_email`: 204
- `source:internal:dc_outreach_emails_safe.csv:excluded_blank_email`: 204
- `excluded_duplicate_email_final`: 103
- `source:internal:catchall_review.csv:excluded_duplicate_within_source`: 88
- `source:internal:sender_ready_hot.csv:excluded_duplicate_within_source`: 84
- `source:internal:discovered_contacts.csv:excluded_duplicate_within_source`: 38
- `source:internal:monday_people_found.csv:excluded_blank_email`: 5
- `projects_without_contact_match`: 5
- `source:internal:sender_ready_warm.csv:excluded_duplicate_within_source`: 4
- `source:internal:monday_low_confidence_domains.csv:excluded_blank_email`: 2

## Matching Tier Counts
- `match_tier_metro`: 35
- `match_tier_soft_company`: 1
- `match_tier_state`: 1
