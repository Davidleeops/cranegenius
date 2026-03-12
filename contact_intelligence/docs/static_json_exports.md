# Static JSON Exports (Phase 2)

## Script
`contact_intelligence/scripts/export_static_json.py`

## Default Output Paths
- `data/static_exports/manpower_profiles.json`
- `data/static_exports/manpower_job_matches.json`
- `data/static_exports/manpower_export_meta.json`

## Field Schema

### `manpower_profiles.json`
Top-level:
- `generated_at` (ISO timestamp)
- `count` (int)
- `profiles` (array)

Profile object fields:
- `id` (int)
- `first` (string)
- `last` (string)
- `initials` (string)
- `role` (string)
- `location` (string)
- `exp` (int)
- `avail` (`now|soon|employed`)
- `certs` (string[])
- `equipment` (string[])
- `union` (string)
- `travel` (string)
- `pay` (string)
- `bio` (string)
- `verified` (bool)
- `daysAgo` (int)
- `matchScore` (0-100 int)
- `matchJobs` (array of `{title, company, score, reason}`)
- `source` (string)
- `email` (string)
- `phone` (string)
- `quality` (0-1 float)

### `manpower_job_matches.json`
Top-level:
- `generated_at` (ISO timestamp)
- `count` (int)
- `matches` (array)

Match object fields:
- `profile_id` (int)
- `profile_name` (string)
- `job_title` (string)
- `company` (string)
- `score` (0-100 int)
- `reason` (string)

### `manpower_export_meta.json`
- `generated_at`
- `paths` (`profiles`, `matches`)
- `schema_version`
- `notes`

## Regeneration Command
```bash
python3 contact_intelligence/scripts/export_static_json.py \
  --db ~/data_runtime/cranegenius_ci.db \
  --output-dir data/static_exports \
  --limit 200
```
