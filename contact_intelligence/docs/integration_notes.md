# Integration Notes — Permit Pipeline ↔ Contact Intelligence

## What Stays Separate
- The live permit pipeline (`src/pipeline.py`) is NOT modified
- This system lives entirely in `contact_intelligence/` 
- No shared code, no shared database file

## How to Connect Them

### 1. Import pipeline output into this system

The permit pipeline outputs `data/verified_contacts.csv` and `data/enriched_companies.csv`.

After a pipeline run:
```
python3 contact_intelligence/scripts/import_csv.py \
  --file data/verified_contacts.csv \
  --source permit_pipeline \
  --type permit \
  --business_line cranegenius

python3 contact_intelligence/scripts/normalize_records.py --source permit_pipeline
python3 contact_intelligence/scripts/dedupe_people_companies.py
```

### 2. Feed domain evidence back to the pipeline (future)

The `domain_evidence` and `canonical_companies` tables can be used by future pipeline stages to:
- Prefer domains already verified in CI over SerpAPI guesses
- Skip companies already confirmed as "wrong industry"
- Use `contact_patterns` to predict email format before verifying

Bridge query example:
```sql
SELECT cc.canonical_name, de.domain_candidate, de.confidence_score
FROM canonical_companies cc
JOIN domain_evidence de ON de.canonical_company_id = cc.canonical_company_id
WHERE de.verified_status = 'verified'
ORDER BY de.confidence_score DESC;
```

### 3. Feed pipeline verification results back

After MillionVerifier runs, log the results:
```
python3 contact_intelligence/scripts/log_feedback.py \
  --file data/mv_results.csv \
  --source millionverifier
```
(Expects columns: `email`, `outcome` — map bounce/valid to bounce/verified_domain)

### 4. Seed data improves domain resolution

Put manually curated company lists in `~/data_runtime/seeds/`:
```
python3 contact_intelligence/scripts/import_seed_data.py \
  --file ~/data_runtime/seeds/crane_companies.csv \
  --source manual_seed_2025
```

After seeding:
```
python3 contact_intelligence/scripts/score_domain_evidence.py
```

The CI system now has higher-confidence domain candidates that the pipeline can query instead of re-running SerpAPI.

## Future: Shared DB Mode

Currently the pipeline and CI system use separate database files.
When ready to unify, the pipeline's `src/pipeline.py` can be pointed at
`~/data_runtime/cranegenius_ci.db` with a `CRANEGENIUS_CI_DB` env variable.
The CI schema is a superset of the pipeline's needs.
