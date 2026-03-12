# TASK.md — CraneGenius Contact Intelligence v2

## Status: COMPLETE — Ready for deployment to ~/Downloads/cranegenius_repo/

---

## Deploy to Your Repo (run from ~/Downloads/cranegenius_repo/)

### 1. Unzip or copy the contact_intelligence/ folder into repo root

After extracting, your repo should have:
```
~/Downloads/cranegenius_repo/contact_intelligence/
```

### 2. Install dependencies

```
pip install openpyxl rapidfuzz python-dotenv
```

### 3. Initialize the database

```
python3 contact_intelligence/scripts/init_db.py
```

Creates: `~/data_runtime/cranegenius_ci.db`

### 4. Import your existing verified contacts

```
python3 contact_intelligence/scripts/import_csv.py --file data/verified_contacts.csv --source permit_pipeline --type permit --business_line cranegenius
```

### 5. Import seed data (crane company reference list)

```
python3 contact_intelligence/scripts/import_seed_data.py --file contact_intelligence/sample_data/sample_seed.csv --source manual_seed_2025
```

### 6. Normalize records

```
python3 contact_intelligence/scripts/normalize_records.py
```

### 7. Deduplicate

```
python3 contact_intelligence/scripts/dedupe_people_companies.py
```

### 8. Score domain evidence

```
python3 contact_intelligence/scripts/score_domain_evidence.py
```

### 9. Export spreadsheets

```
python3 contact_intelligence/scripts/export_views.py
```

### 10. Search

```
python3 contact_intelligence/scripts/search_cli.py --state TX --verified
python3 contact_intelligence/scripts/search_cli.py --sector "Data Center"
python3 contact_intelligence/scripts/search_cli.py --role operations --region Southwest
```

### 11. Backup

```
python3 contact_intelligence/scripts/backup_db.py
```

---

## Add to .gitignore

Add these lines to ~/Downloads/cranegenius_repo/.gitignore:
```
data_runtime/
*.db
credentials/
__pycache__/
```

---

## Next Agent Tasks

1. Build `assign_top_targets.py` — populate top_target_lists + top_target_entries by sector score
2. Add auto-import wrapper that runs after each permit pipeline run
3. Build target_score computation (weighted: sector + signals + project stage + contact availability)
4. Add `--sector` auto-assignment when importing permit CSVs
5. Consider a lightweight Streamlit dashboard layer for internal CRM use
