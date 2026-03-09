# TASK.md — CraneGenius Active Task

## Status: Contact Intelligence System v2 — OPERATIONAL

### Completed This Session
- [x] Root cause found: `verified_contacts.csv` has only 5 columns (verifier output only)
- [x] `fix_ci.py` written to repo root — reads `candidates.csv` + `verified_contacts.csv`, joins on email
- [x] DB loaded: 129 companies, 2,600 contacts (Dallas + Chicago combined)
- [x] Exports confirmed working: `top_100_targets.xlsx` (100 rows), `verified_contacts.xlsx` (10 rows)
- [x] Dallas junk data identified and filtered out (2,432 residential/small contractor contacts removed)
- [x] CI system left with 21 clean Chicago GC companies, 168 contacts, 10 verified
- [x] Sector assignment working (66 companies got sector_id via keyword matching)

### Known Data Quality Issues
- All candidates use `role_inbox` generation method — emails are pattern-guessed, not sourced from real contacts
- Company names in candidates.csv have address strings embedded (pipeline upstream issue)
- Dallas permit data produces residential/small contractors — wrong target profile
- Only 10 verified contacts in DB (all Chicago)

### Next Task: Improve Pipeline Output Quality
The CI system works. The bottleneck is pipeline data quality. Priority order:

1. **Re-run pipeline for Chicago only** to get fresh clean candidates
   - Run: `python3 -m src.pipeline` from repo root
   - Chicago Socrata API returns permittee first/last name — use these for `role_named` email generation
   - Target: Replace `role_inbox` with actual named contacts from permit permittee fields

2. **Update `normalize_records.py`** to use the logic from `fix_ci.py`
   - Current `normalize_records.py` still looks for wrong column names
   - Replace with: read `candidates.csv` + `verified_contacts.csv`, join on email
   - File: `contact_intelligence/scripts/normalize_records.py`

3. **NYC pipeline run** — NYC DOB API has best permittee name data
   - Produces `role_named` contacts naturally
   - Add NYC to pipeline targets

4. **Import wrapper** — auto-run CI import after each pipeline run
   - Script: `contact_intelligence/scripts/auto_import.py`
   - Should call: import_csv → fix_ci logic → export_views in sequence

### Key File Locations
- DB: `~/data_runtime/cranegenius_ci.db`
- Working import script: `~/Downloads/cranegenius_repo/fix_ci.py` (use this, not normalize_records.py)
- Exports: `~/data_runtime/exports/`
- Pipeline output: `data/candidates.csv` (2600 rows, mostly Dallas junk — needs re-run)

### Import Command (working)
```bash
cd ~/Downloads/cranegenius_repo
python3 fix_ci.py
python3 contact_intelligence/scripts/export_views.py
```
