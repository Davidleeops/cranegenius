# CraneGenius Contact Intelligence System v2

Local-first, SQLite-backed intelligence system for CraneGenius.
Three-layer architecture: CRM/Marketplace | Reference/Memory | Learning/Evaluation.

---

## Quickstart (from repo root)

```
pip install -r contact_intelligence/requirements.txt
python3 contact_intelligence/scripts/init_db.py
```

Then copy your data to `~/data_runtime/imports/` and run:

```
python3 contact_intelligence/scripts/import_csv.py \
  --file ~/data_runtime/imports/verified_contacts.csv \
  --source permit_pipeline_chicago \
  --type permit \
  --business_line cranegenius

python3 contact_intelligence/scripts/normalize_records.py
python3 contact_intelligence/scripts/dedupe_people_companies.py
python3 contact_intelligence/scripts/export_views.py
```

---

## Full Workflow

| Step | Script | What It Does |
|---|---|---|
| 1 | `init_db.py` | Create DB + runtime folders + seed 20 sectors |
| 2a | `import_csv.py` | Import contact CSV → source_records |
| 2b | `import_seed_data.py` | Import seed CSV → canonical_companies + domain_evidence |
| 3 | `normalize_records.py` | Normalize → companies + contacts master tables |
| 4 | `dedupe_people_companies.py` | Merge duplicates |
| 5 | `score_domain_evidence.py` | Score domain candidates |
| 6 | `update_contact_patterns.py` | Infer email patterns |
| 7 | `export_views.py` | Generate .xlsx exports |
| 8 | `search_cli.py` | Search from terminal |
| 9 | `run_gold_truth_checks.py` | Benchmark accuracy |
| 10 | `log_feedback.py` | Log bounces/replies/outcomes |
| 11 | `backup_db.py` | Timestamped backup |

---

## Database Location

Default: `~/data_runtime/cranegenius_ci.db`

Override: `export CRANEGENIUS_CI_DB=/your/path/ci.db`

Runtime folders auto-created at init:
```
~/data_runtime/
  cranegenius_ci.db
  imports/
  exports/
  backups/
  logs/
  seeds/
```

---

## Search Examples

```
python3 contact_intelligence/scripts/search_cli.py --state TX --verified
python3 contact_intelligence/scripts/search_cli.py --sector "Data Center" --state TX
python3 contact_intelligence/scripts/search_cli.py --role operations --region Southwest
python3 contact_intelligence/scripts/search_cli.py --crane_type crawler --state LA
python3 contact_intelligence/scripts/search_cli.py --tier 1 --format csv > tier1.csv
python3 contact_intelligence/scripts/search_cli.py --signal industrial_turnaround
python3 contact_intelligence/scripts/search_cli.py --company "leavitt" --has_phone
```

---

## Feedback Loop

```
python3 contact_intelligence/scripts/log_feedback.py \
  --email jane@acme.com --outcome bounce --source plusvibe_jan

python3 contact_intelligence/scripts/log_feedback.py \
  --domain acme.com --outcome verified_domain --source manual

python3 contact_intelligence/scripts/score_domain_evidence.py
python3 contact_intelligence/scripts/update_contact_patterns.py
```

---

## Exports Generated

| File | Contents |
|---|---|
| `top_100_targets.xlsx` | Highest-confidence contacts, all sectors |
| `verified_contacts.xlsx` | Verified-email contacts only |
| `shared_contacts.xlsx` | Contacts in 2+ business lines |
| `high_confidence_targets.xlsx` | Confidence ≥ 0.7 |
| `data_center_targets.xlsx` | Data center projects + contacts |
| `industrial_turnaround.xlsx` | Industrial turnaround signals + contacts |
| `top100_by_sector.xlsx` | One tab per active sector |

---

## Do NOT Commit to Git

```
~/data_runtime/         ← entire folder
*.db files
Real contact CSVs
.env / credential files
```

Add to `.gitignore`:
```
data_runtime/
*.db
credentials/
__pycache__/
```

---

## Architecture Docs

- `docs/data_dictionary.md` — all tables and fields
- `docs/workflow.md` — end-to-end process
- `docs/matching_rules.md` — dedup logic
- `docs/integration_notes.md` — how this connects to the permit pipeline
