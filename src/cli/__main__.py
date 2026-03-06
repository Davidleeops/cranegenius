from __future__ import annotations
import argparse, json, sys, uuid, logging
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from dotenv import load_dotenv
load_dotenv()

from src.llm.router import generate_json, LLMError
from src.llm.schemas import JOBSPEC_SCHEMA
from src.llm.context_loader import inject_context, load_project_context, save_run_state, update_context_note

log = logging.getLogger("cranegenius.cli")

# ── Config paths (same as pipeline.py) ────────────────────────
SOURCES_YAML  = "config/sources.yaml"
KEYWORDS_YAML = "config/keywords.yaml"
SCORING_YAML  = "config/scoring.yaml"
CRAWLER_YAML  = "config/crawler.yaml"

# ── Job planner ────────────────────────────────────────────────
def plan_job(natural_language: str) -> dict:
    messages = inject_context([{
        "role": "user",
        "content": (
            f"Convert this request into a CraneGenius JobSpec JSON.\n"
            f"Request: {natural_language}\n"
            f"Schema: {json.dumps(JOBSPEC_SCHEMA)}\n"
            f"Return ONLY valid JSON."
        )
    }])
    result = generate_json("plan_job", JOBSPEC_SCHEMA, messages)
    return result.parsed

# ── Real pipeline stage runners ────────────────────────────────
def run_full_pipeline(jobspec: dict) -> dict:
    """
    Calls your real pipeline functions in order.
    Mirrors the logic in src/pipeline.py but driven by JobSpec.
    """
    import logging
    from src.utils import load_yaml, save_csv, setup_logging
    from src.ingest import ingest_sources
    from src.parse_normalize import normalize_records
    from src.score_filter import score_and_filter
    from src.company_resolver import resolve_domains
    from src.domain_enricher_claude import enrich_domains_with_claude
    from src.site_contact_miner import mine_contacts
    from src.candidate_builder import build_candidates
    from src.verify_millionverifier import verify_with_millionverifier
    from src.exporter import export_sender_lists
    from src.sheets_exporter import export_to_sheets

    setup_logging()
    stages    = jobspec.get("pipeline_stages", [])
    threshold = jobspec.get("crane_score_threshold", 0.6)
    counts    = {}

    # Stage 1 — Ingest
    if "permit_ingestion" in stages:
        print("\n  [Stage 1] Ingesting permits...")
        raw_df = ingest_sources(SOURCES_YAML)
        save_csv(raw_df, "data/raw_records.csv")
        counts["permits_ingested"] = len(raw_df)
        print(f"  ✓ {len(raw_df)} raw records")
    else:
        import pandas as pd
        raw_df = pd.read_csv("data/raw_records.csv")

    # Stage 2 — Normalize
    if "normalization" in stages:
        print("\n  [Stage 2] Normalizing...")
        normalized_df, _ = normalize_records(raw_df)
        save_csv(normalized_df, "data/normalized_records.csv")
        counts["permits_normalized"] = len(normalized_df)
        print(f"  ✓ {len(normalized_df)} normalized")
    else:
        import pandas as pd
        normalized_df = pd.read_csv("data/normalized_records.csv")

    # Stage 3 — Score
    if "crane_scoring" in stages:
        print("\n  [Stage 3] Scoring for crane likelihood...")
        scored_df = score_and_filter(normalized_df, KEYWORDS_YAML, SCORING_YAML)
        save_csv(scored_df, "data/scored_records.csv")
        scoring_cfg = load_yaml(SCORING_YAML)
        threshold_warm = scoring_cfg.get("scoring", {}).get("threshold_warm", 4)
        enrichment_queue = scored_df[scored_df["lift_probability_score"] >= threshold_warm].copy()
        save_csv(enrichment_queue, "data/enrichment_queue.csv")
        hot_count  = len(scored_df[scored_df["lift_probability_score"] >= 7])
        warm_count = len(enrichment_queue)
        counts["permits_scored"] = len(scored_df)
        counts["hot_permits"]    = hot_count
        counts["warm_permits"]   = warm_count
        print(f"  ✓ hot={hot_count} warm={warm_count}")
    else:
        import pandas as pd
        enrichment_queue = pd.read_csv("data/enrichment_queue.csv")

    # Stage 4 — Domain resolution
    if "domain_resolution" in stages:
        print("\n  [Stage 4] Resolving contractor domains...")
        enriched_df = resolve_domains(enrichment_queue)
        enriched_df = enrich_domains_with_claude(enriched_df)
        save_csv(enriched_df, "data/enriched_companies.csv")
        resolved = enriched_df["contractor_domain"].notna().sum()
        counts["domains_resolved"] = int(resolved)
        print(f"  ✓ {resolved} domains resolved")
    else:
        import pandas as pd
        enriched_df = pd.read_csv("data/enriched_companies.csv")

    # Stage 5 — Contact mining
    if "contact_mining" in stages:
        print("\n  [Stage 5] Mining contacts from company sites...")
        contacts_df, patterns_df = mine_contacts(enriched_df, CRAWLER_YAML)
        save_csv(contacts_df, "data/discovered_contacts.csv")
        save_csv(patterns_df, "data/domain_email_patterns.csv")
        counts["contacts_found"] = len(contacts_df)
        print(f"  ✓ {len(contacts_df)} contacts found")
    else:
        import pandas as pd
        contacts_df  = pd.read_csv("data/discovered_contacts.csv")
        patterns_df  = pd.read_csv("data/domain_email_patterns.csv")

    # Stage 6 — Build candidates
    if "email_generation" in stages:
        print("\n  [Stage 6] Building email candidates...")
        candidates_df = build_candidates(
            enriched_df, KEYWORDS_YAML,
            contacts_df=contacts_df,
            patterns_df=patterns_df,
        )
        save_csv(candidates_df, "data/candidates.csv")
        counts["emails_generated"] = len(candidates_df)
        print(f"  ✓ {len(candidates_df)} candidates built")
    else:
        import pandas as pd
        candidates_df = pd.read_csv("data/candidates.csv")

    # Stage 7 — Verify (only real domains)
    if "verification" in stages:
        print("\n  [Stage 7] Verifying with MillionVerifier...")
        if "domain_resolution_source" in enriched_df.columns:
            real_domains = enriched_df[
                enriched_df["domain_resolution_source"].isin(
                    ["seed_partial", "seed", "enrichment_confident"]
                )
            ]["contractor_domain"].dropna().unique()
            verify_candidates = candidates_df[
                candidates_df["contractor_domain"].isin(real_domains)
            ].copy()
            print(f"  Filtering: {len(candidates_df)} → {len(verify_candidates)} (skipping generated domains)")
        else:
            verify_candidates = candidates_df
        verified_df = verify_with_millionverifier(verify_candidates)
        save_csv(verified_df, "data/verified_contacts.csv")
        valid_count = int((verified_df.get("mv_result","") == "valid").sum()) if "mv_result" in verified_df.columns else 0
        counts["emails_verified"] = valid_count
        print(f"  ✓ {valid_count} verified valid")
    else:
        import pandas as pd
        verified_df = pd.read_csv("data/verified_contacts.csv")

    # Stage 8 — Export
    if "sheets_export" in stages:
        print("\n  [Stage 8] Exporting sender-ready lists...")
        scoring_cfg   = load_yaml(SCORING_YAML)
        scoring       = scoring_cfg.get("scoring", {})
        threshold_hot  = scoring.get("threshold_hot",  7)
        threshold_warm = scoring.get("threshold_warm", 5)
        # Join candidates (has email_candidate) with enriched (has lift_probability_score)
        import pandas as pd
        candidates_for_export = pd.read_csv("data/candidates.csv")
        scored_enriched = candidates_for_export.merge(
            enriched_df[["contractor_domain","lift_probability_score","score_hits",
                         "project_address","project_city","project_state",
                         "jurisdiction","permit_or_record_id","record_status"]],
            on="contractor_domain", how="left"
        )
        hot_df, warm_df, catchall_df, qa = export_sender_lists(
            scored_enriched, verified_df, threshold_hot, threshold_warm
        )
        save_csv(hot_df,      "data/sender_ready_hot.csv")
        save_csv(warm_df,     "data/sender_ready_warm.csv")
        save_csv(catchall_df, "data/catchall_review.csv")
        export_to_sheets(warm_df, hot_df, catchall_df)
        counts["exported_hot"]      = len(hot_df)
        counts["exported_warm"]     = len(warm_df)
        counts["exported_catchall"] = len(catchall_df)
        print(f"  ✓ hot={len(hot_df)} warm={len(warm_df)} catchall={len(catchall_df)}")

    return counts

# ── CLI executor ───────────────────────────────────────────────
def execute_jobspec(jobspec: dict, dry_run: bool = False) -> dict:
    run_id     = str(uuid.uuid4())[:8]
    start_time = datetime.now()

    print(f"\n{'='*60}")
    print(f"CraneGenius Run [{run_id}]")
    print(f"Goal:    {jobspec.get('goal','')}")
    print(f"Markets: {', '.join(jobspec.get('markets', []))}")
    print(f"Stages:  {' → '.join(jobspec.get('pipeline_stages', []))}")
    print(f"{'='*60}")

    counts = {}

    if dry_run:
        print("\n[DRY RUN — showing what would execute]\n")
        for stage in jobspec.get("pipeline_stages", []):
            print(f"  (dry-run) Would run: {stage}")
        status = "dry_run"
    else:
        try:
            counts = run_full_pipeline(jobspec)
            status = "success"
        except Exception as e:
            print(f"\n  ✗ Pipeline error: {e}")
            import traceback; traceback.print_exc()
            status = "failed"

    elapsed = (datetime.now() - start_time).total_seconds()

    run_summary = {
        "run_id":    run_id,
        "goal":      jobspec.get("goal"),
        "markets":   jobspec.get("markets", []),
        "stages":    jobspec.get("pipeline_stages", []),
        "counts":    counts,
        "status":    status,
        "runtime_s": elapsed,
        "jobspec":   jobspec,
    }

    if not dry_run:
        sf = save_run_state(run_summary)
        print(f"\n  [context] Run state saved → {sf}")

    return run_summary

# ── Main ───────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="CraneGenius CLI")
    parser.add_argument("request",       nargs="*")
    parser.add_argument("--dry-run",     action="store_true")
    parser.add_argument("--plan-only",   action="store_true")
    parser.add_argument("--note",        type=str)
    parser.add_argument("--show-context",action="store_true")
    parser.add_argument("--show-jobspec",action="store_true")
    args = parser.parse_args()

    if args.note:
        update_context_note(args.note)
        print(f"✓ Note saved: {args.note}")
        return

    if args.show_context:
        print(load_project_context())
        return

    if not args.request:
        parser.print_help()
        sys.exit(1)

    natural_language = " ".join(args.request)
    print(f"\nCraneGenius CLI\nRequest: {natural_language}\n")
    print("Planning job with Claude...")

    try:
        jobspec = plan_job(natural_language)
    except LLMError as e:
        print(f"\nFATAL: {e}")
        sys.exit(1)

    if args.show_jobspec or args.plan_only or args.dry_run:
        print("\nJobSpec:")
        print(json.dumps(jobspec, indent=2))

    if args.plan_only:
        return

    run_summary = execute_jobspec(jobspec, dry_run=args.dry_run)

    print(f"\n{'='*60}")
    print(f"Run complete [{run_summary['run_id']}] — {run_summary['status'].upper()}")
    if run_summary["counts"]:
        print("Counts:")
        for k, v in run_summary["counts"].items():
            print(f"  {k}: {v}")
    print(f"Time: {run_summary['runtime_s']:.1f}s")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    main()
