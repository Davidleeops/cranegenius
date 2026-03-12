#!/usr/bin/env bash
set -euo pipefail

LIVE_REPO="/Users/lemueldavidleejr/Downloads/cranegenius_repo"
BACKUP_REPO="/Users/lemueldavidleejr/Desktop/cranegenius/cranegenius_repo_backup_unreviewed"
TS="$(date +%Y%m%d_%H%M%S)"
APPLY=0

if [[ "${1:-}" == "--apply" ]]; then
  APPLY=1
fi

MERGE_NOW_FILES=(
  "config/routing_schema.json"
  "config/send_selection.yaml"
  "config/signal_schema.json"
  "config/supplier_schema.json"
  "src/company_selector.py"
  "src/domain_dedupe.py"
  "src/domain_discovery.py"
  "src/monday_campaign_fast_path.py"
  "src/monday_company_list_fast_path.py"
  "src/monday_individual_contact_generation.py"
  "src/monday_people_pipeline.py"
  "src/people_discovery.py"
  "src/people_email_generator.py"
  "src/scrapers/bid_board_scraper.py"
  "src/scrapers/contractor_directory_scraper.py"
  "src/scrapers/industrial_project_scraper.py"
  "src/scrapers/permit_multi_city_scraper.py"
  "scripts/domain_discovery_benchmark.py"
  "scripts/run_and_prepare_learning.py"
  "tests/test_domain_discovery.py"
  "tests/test_people_email_generator.py"
  "src/candidate_builder.py"
  "src/ingest.py"
  "src/parse_normalize.py"
  "src/pipeline.py"
  "src/score_filter.py"
  "src/scrapers/__init__.py"
)

echo "Live repo:   $LIVE_REPO"
echo "Backup repo: $BACKUP_REPO"
if [[ $APPLY -eq 0 ]]; then
  echo "Mode: DRY-RUN (no changes)"
  echo "Run with --apply to execute"
else
  echo "Mode: APPLY"
fi

echo
for rel in "${MERGE_NOW_FILES[@]}"; do
  src="$BACKUP_REPO/$rel"
  dst="$LIVE_REPO/$rel"

  if [[ ! -f "$src" ]]; then
    echo "[SKIP missing in backup] $rel"
    continue
  fi

  if [[ $APPLY -eq 0 ]]; then
    if [[ -f "$dst" ]]; then
      echo "[PLAN overwrite+backup] $rel -> $rel.bak.$TS"
    else
      echo "[PLAN copy new] $rel"
    fi
    continue
  fi

  mkdir -p "$(dirname "$dst")"
  if [[ -f "$dst" ]]; then
    cp "$dst" "$dst.bak.$TS"
    echo "[BACKUP] $dst.bak.$TS"
  fi
  cp "$src" "$dst"
  echo "[COPIED] $rel"
done

echo
echo "Done."
