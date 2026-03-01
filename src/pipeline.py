"""
CraneGenius Intent Pipeline — Main Orchestrator
Dark 30 Ventures

Run order:
  Stage 1: Ingest → raw_records.csv
  Stage 2: Normalize → normalized_records.csv
  Stage 3: Score → scored_records.csv + enrichment_queue.csv
  Stage 4: Resolve domains → enriched_companies.csv
  Stage 5: Mine contacts → discovered_contacts.csv + domain_email_patterns.csv
  Stage 6: Build candidates → candidates.csv
  Stage 7: Verify → verified_contacts.csv
  Stage 8: Export → sender_ready_hot.csv, sender_ready_warm.csv, catchall_review.csv
  Stage 9: QA report + gate check → qa_report.json
"""
from __future__ import annotations

import json
import logging
import sys

from .ingest import ingest_sources
from .parse_normalize import normalize_records
from .score_filter import score_and_filter
from .company_resolver import resolve_domains
from .domain_enricher_claude import enrich_domains_with_claude
from .site_contact_miner import mine_contacts
from .candidate_builder import build_candidates
from .verify_millionverifier import verify_with_millionverifier
from .exporter import export_sender_lists
from .monitor import check_gates, load_state, save_state, update_source_state
from .utils import load_yaml, save_csv, setup_logging

log = logging.getLogger("cranegenius.pipeline")

SOURCES_YAML = "config/sources.yaml"
KEYWORDS_YAML = "config/keywords.yaml"
SCORING_YAML = "config/scoring.yaml"
CRAWLER_YAML = "config/crawler.yaml"


def main() -> None:
    setup_logging()
    log.info("=" * 60)
    log.info("CraneGenius Intent Pipeline — Dark 30 Ventures")
    log.info("=" * 60)

    scoring_cfg = load_yaml(SCORING_YAML)
    scoring = scoring_cfg.get("scoring", {})
    state = load_state()

    # ── STAGE 1: INGEST ───────────────────────────────────────────
    log.info("\n[Stage 1] Ingesting sources...")
    raw_df = ingest_sources(SOURCES_YAML)
    save_csv(raw_df, "data/raw_records.csv")

    if raw_df.empty:
        log.error("No records ingested. Check config/sources.yaml — are any sources enabled?")
        sys.exit(0)  # allow pipeline to complete even with 0 records

    # Update source monitoring state
    sources_cfg = load_yaml(SOURCES_YAML).get("sources", [])
    enabled_sources = [s for s in sources_cfg if s.get("enabled")]
    state = update_source_state(raw_df, enabled_sources, state)
    save_state(state)

    # ── STAGE 2: NORMALIZE ────────────────────────────────────────
    log.info("\n[Stage 2] Normalizing records...")
    normalized_df, errors_df = normalize_records(raw_df)
    save_csv(normalized_df, "data/normalized_records.csv")
    if not errors_df.empty:
        save_csv(errors_df, "data/parsing_errors.csv")

    # ── STAGE 3: SCORE + FILTER ───────────────────────────────────
    log.info("\n[Stage 3] Scoring records...")
    scored_df = score_and_filter(normalized_df, KEYWORDS_YAML, SCORING_YAML)
    save_csv(scored_df, "data/scored_records.csv")

    threshold_warm = int(scoring.get("threshold_warm", 5))
    threshold_hot = int(scoring.get("threshold_hot", 7))

    enrichment_queue = scored_df[scored_df["lift_probability_score"] >= threshold_warm].copy()
    save_csv(enrichment_queue, "data/enrichment_queue.csv")
    log.info("Enrichment queue: %d records at score >= %d", len(enrichment_queue), threshold_warm)

    if enrichment_queue.empty:
        log.warning("Enrichment queue is empty — no records scored high enough. "
                    "Check keyword matches against your raw data.")

    # ── STAGE 4: RESOLVE DOMAINS ──────────────────────────────────
    log.info("\n[Stage 4] Resolving contractor domains...")
    enriched_df = resolve_domains(enrichment_queue)
    save_csv(enriched_df, "data/enriched_companies.csv")

    # ── STAGE 4b: CLAUDE DOMAIN ENRICHMENT ──────────────────────
    log.info("[Stage 4b] Enriching unresolved domains via Claude...")
    enriched_df = enrich_domains_with_claude(enriched_df)
    save_csv(enriched_df, "data/enriched_companies.csv")

    # ── STAGE 5: MINE CONTACTS ────────────────────────────────────
    log.info("\n[Stage 5] Mining contacts from company sites...")
    contacts_df, patterns_df = mine_contacts(enriched_df, CRAWLER_YAML)
    save_csv(contacts_df, "data/discovered_contacts.csv")
    save_csv(patterns_df, "data/domain_email_patterns.csv")

    # ── STAGE 6: BUILD CANDIDATES ─────────────────────────────────
    log.info("\n[Stage 6] Building email candidates...")
    candidates_df = build_candidates(
        enriched_df,
        KEYWORDS_YAML,
        contacts_df=contacts_df,
        patterns_df=patterns_df,
    )
    save_csv(candidates_df, "data/candidates.csv")

    if candidates_df.empty:
        log.warning("No candidates generated — domain resolution may have failed. "
                    "Populate data/company_domain_seed.csv and re-run.")

    # ── STAGE 7: VERIFY ───────────────────────────────────────────
    log.info("\n[Stage 7] Verifying emails with MillionVerifier...")
    verified_df = verify_with_millionverifier(candidates_df)
    save_csv(verified_df, "data/verified_contacts.csv")

    # ── STAGE 8: MERGE + EXPORT ───────────────────────────────────
    log.info("\n[Stage 8] Exporting sender-ready lists...")
    if candidates_df.empty or "contractor_domain" not in enriched_df.columns:
        log.warning("No candidates to export — pipeline complete with 0 sender-ready leads")
        sys.exit(0)
    scored_candidates = enriched_df.merge(
        candidates_df, on="contractor_domain", how="left"
    )

    hot_df, warm_df, catchall_df, qa = export_sender_lists(
        scored_candidates,
        verified_df,
        threshold_hot=threshold_hot,
        threshold_warm=threshold_warm,
    )

    # ── STAGE 9: GATE CHECK + QA ──────────────────────────────────
    log.info("\n[Stage 9] Running monitoring gates...")
    gate_report = check_gates(qa, scoring_cfg)
    qa["gate_report"] = gate_report

    with open("data/qa_report.json", "w", encoding="utf-8") as f:
        json.dump(qa, f, ensure_ascii=False, indent=2)

    if gate_report["halt"]:
        log.error("Pipeline halted by monitoring gate. Sender lists NOT written.")
        log.error("Fix the issues in qa_report.json before sending.")
        sys.exit(0)  # gate halt is expected, not a crash

    # Only write sender lists if gates passed
    save_csv(hot_df, "data/sender_ready_hot.csv")
    save_csv(warm_df, "data/sender_ready_warm.csv")
    save_csv(catchall_df, "data/catchall_review.csv")

    log.info("\n" + "=" * 60)
    log.info("Pipeline complete.")
    log.info("  Hot list:    %d records → data/sender_ready_hot.csv", len(hot_df))
    log.info("  Warm list:   %d records → data/sender_ready_warm.csv", len(warm_df))
    log.info("  Catchall:    %d records → data/catchall_review.csv (your call)", len(catchall_df))
    log.info("  QA report:   data/qa_report.json")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
