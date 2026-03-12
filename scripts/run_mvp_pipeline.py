#!/usr/bin/env python3
"""
End-to-end CraneGenius MVP orchestrator.

This standardizes the currently working project-intelligence flow:
1. init_db
2. scrape_permits_public
3. scrape_jobs_public
4. normalize_and_match_feeds
5. build_project_intelligence
6. export_project_intelligence

Each stage declares explicit inputs and outputs. The orchestrator writes:
- operator summary markdown
- machine-readable run summary json
- stage-level validation results
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Sequence


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = Path(os.environ.get("CRANEGENIUS_CI_DB", Path.home() / "data_runtime" / "cranegenius_ci.db"))
DEFAULT_EXPORT_DIR = Path(os.environ.get("CRANEGENIUS_EXPORT_DIR", Path.home() / "data_runtime" / "exports"))
DEFAULT_RUNS_DIR = ROOT / "runs" / "mvp_pipeline"
DEFAULT_JSON_EXPORT_DIR = ROOT / "data"
DEFAULT_JOBS_OUTPUT = ROOT / "data" / "jobs_imported.json"
DEFAULT_PERMITS_OUTPUT = ROOT / "data" / "opportunities" / "permits_imported.json"


@dataclass
class StageDefinition:
    name: str
    command: List[str]
    outputs: List[Path] = field(default_factory=list)
    required_outputs: List[Path] = field(default_factory=list)
    optional_outputs: List[Path] = field(default_factory=list)
    description: str = ""


def configure_logging(log_path: Path) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_parent(paths: Iterable[Path]) -> None:
    for path in paths:
        path.parent.mkdir(parents=True, exist_ok=True)


def load_json(path: Path) -> Dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def count_table_rows(db_path: Path, table: str) -> int:
    if not db_path.exists():
        return 0
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.execute(f"SELECT COUNT(*) FROM {table}")
        row = cur.fetchone()
        return int(row[0] if row else 0)
    except sqlite3.Error:
        return 0
    finally:
        conn.close()


def compute_stage_metrics(name: str, db_path: Path, jobs_path: Path, permits_path: Path, export_dir: Path, json_output_dir: Path) -> Dict[str, object]:
    metrics: Dict[str, object] = {}
    if name == "scrape_permits":
        payload = load_json(permits_path)
        metrics["permit_candidate_rows"] = int(payload.get("opportunity_candidates", 0))
        metrics["permit_total_rows"] = int(payload.get("rows_fetched", 0))
        metrics["used_previous_output"] = bool(payload.get("used_previous_output", False))
    elif name == "scrape_jobs":
        payload = load_json(jobs_path)
        metrics["jobs_count"] = int(payload.get("count", 0))
        metrics["jobs_raw_rows"] = int(payload.get("raw_rows", 0))
        metrics["used_previous_output"] = bool(payload.get("used_previous_output", False))
        metrics["used_seed_catalog"] = bool(payload.get("used_seed_catalog", False))
    elif name == "normalize_match":
        for table in ("jobs_feed_items", "opportunity_feed_items", "manpower_profiles", "job_contact_matches", "opportunity_company_matches", "manpower_job_matches"):
            metrics[table] = count_table_rows(db_path, table)
    elif name == "build_project_intelligence":
        for table in ("signal_events", "project_candidates", "project_signal_links", "permit_signal_items", "jobs_signal_items"):
            metrics[table] = count_table_rows(db_path, table)
    elif name == "export_project_intelligence":
        static_dir = json_output_dir / "static_exports"
        metrics["top_project_candidates_records"] = len(load_json(static_dir / "top_project_candidates.json")) if (static_dir / "top_project_candidates.json").exists() else 0
        metrics["recommended_expansion_candidates_records"] = len(load_json(static_dir / "recommended_expansion_candidates.json")) if (static_dir / "recommended_expansion_candidates.json").exists() else 0
        metrics["export_files_present"] = sorted([p.name for p in export_dir.glob("*") if p.is_file()])[:25]
    return metrics


def validate_stage_outputs(stage: StageDefinition) -> List[Dict[str, object]]:
    results = []
    for output in stage.required_outputs:
        exists = output.exists()
        size = output.stat().st_size if exists else 0
        results.append(
            {
                "path": str(output),
                "required": True,
                "exists": exists,
                "size_bytes": size,
            }
        )
    for output in stage.optional_outputs:
        exists = output.exists()
        size = output.stat().st_size if exists else 0
        results.append(
            {
                "path": str(output),
                "required": False,
                "exists": exists,
                "size_bytes": size,
            }
        )
    return results


def summarize_operator_view(stage_results: Sequence[Dict[str, object]], final_metrics: Dict[str, object]) -> str:
    lines = [
        "# MVP Pipeline Run Summary",
        "",
        f"- Generated: `{utc_now()}`",
        f"- Overall status: `{final_metrics['status']}`",
        f"- Database: `{final_metrics['db_path']}`",
        f"- Export directory: `{final_metrics['export_dir']}`",
        "",
        "## Stage Status",
        "",
    ]
    for stage in stage_results:
        lines.append(f"- `{stage['name']}`: `{stage['status']}` in `{stage['duration_seconds']:.2f}s`")
        if stage.get("failure_reason"):
            lines.append(f"  Failure: {stage['failure_reason']}")
    lines.extend(
        [
            "",
            "## Business Output",
            "",
            f"- Top project candidates: `{final_metrics.get('top_project_candidates_records', 0)}`",
            f"- Recommended expansion candidates: `{final_metrics.get('recommended_expansion_candidates_records', 0)}`",
            f"- Project candidate rows in DB: `{final_metrics.get('project_candidates', 0)}`",
            f"- Signal events in DB: `{final_metrics.get('signal_events', 0)}`",
            "",
            "## Next Operator Check",
            "",
            f"- Review `{final_metrics['operator_exports_hint']}`",
            f"- Review `{final_metrics['machine_summary_path']}` for stage-level validations and metrics",
        ]
    )
    return "\n".join(lines) + "\n"


def run_stage(stage: StageDefinition, log: logging.Logger, db_path: Path, jobs_path: Path, permits_path: Path, export_dir: Path, json_output_dir: Path) -> Dict[str, object]:
    ensure_parent(stage.outputs)
    started = time.time()
    log.info("Starting stage %s", stage.name)
    proc = subprocess.run(stage.command, cwd=str(ROOT), capture_output=True, text=True)
    validations = validate_stage_outputs(stage)
    required_missing = [item["path"] for item in validations if item["required"] and not item["exists"]]
    metrics = compute_stage_metrics(stage.name, db_path, jobs_path, permits_path, export_dir, json_output_dir)
    result = {
        "name": stage.name,
        "description": stage.description,
        "command": stage.command,
        "status": "success" if proc.returncode == 0 and not required_missing else "failed",
        "returncode": proc.returncode,
        "duration_seconds": time.time() - started,
        "stdout": proc.stdout.strip(),
        "stderr": proc.stderr.strip(),
        "outputs": validations,
        "metrics": metrics,
        "failure_reason": "",
    }
    if proc.returncode != 0:
        result["failure_reason"] = f"non-zero exit code {proc.returncode}"
    elif required_missing:
        result["failure_reason"] = f"missing required outputs: {', '.join(required_missing)}"
    log.info("Finished stage %s with status=%s", stage.name, result["status"])
    return result


def build_stages(db_path: Path, jobs_path: Path, permits_path: Path, export_dir: Path, json_output_dir: Path) -> List[StageDefinition]:
    static_exports = json_output_dir / "static_exports"
    return [
        StageDefinition(
            name="init_db",
            description="Initialize SQLite runtime and base schema.",
            command=["python3", "contact_intelligence/scripts/init_db.py", "--db", str(db_path)],
            outputs=[db_path],
            required_outputs=[db_path],
        ),
        StageDefinition(
            name="scrape_permits",
            description="Fetch public permit opportunities into normalized JSON.",
            command=["python3", "scripts/scrape_permits_public.py", "--output", str(permits_path)],
            outputs=[permits_path],
            required_outputs=[permits_path],
        ),
        StageDefinition(
            name="scrape_jobs",
            description="Fetch public job signals into normalized JSON.",
            command=["python3", "scripts/scrape_jobs_public.py", "--output", str(jobs_path)],
            outputs=[jobs_path],
            required_outputs=[jobs_path],
        ),
        StageDefinition(
            name="normalize_match",
            description="Load feeds into CI tables and compute deterministic match candidates.",
            command=[
                "python3",
                "contact_intelligence/scripts/normalize_and_match_feeds.py",
                "--db",
                str(db_path),
                "--jobs",
                str(jobs_path),
                "--permits",
                str(permits_path),
            ],
            outputs=[db_path],
            required_outputs=[db_path],
        ),
        StageDefinition(
            name="build_project_intelligence",
            description="Create signal events and project candidate intelligence records.",
            command=[
                "python3",
                "contact_intelligence/scripts/build_project_intelligence.py",
                "--db",
                str(db_path),
                "--repo-root",
                str(ROOT),
            ],
            outputs=[db_path],
            required_outputs=[db_path],
        ),
        StageDefinition(
            name="export_project_intelligence",
            description="Produce operator exports and static JSON payloads.",
            command=[
                "python3",
                "contact_intelligence/scripts/export_project_intelligence.py",
                "--db",
                str(db_path),
                "--output",
                str(export_dir),
                "--json-output",
                str(json_output_dir),
            ],
            outputs=[
                static_exports / "project_candidates.json",
                static_exports / "top_project_candidates.json",
                static_exports / "mini_opportunity_candidates.json",
                static_exports / "recommended_expansion_candidates.json",
            ],
            required_outputs=[
                static_exports / "project_candidates.json",
                static_exports / "top_project_candidates.json",
            ],
            optional_outputs=[
                export_dir / "top_project_candidates.xlsx",
                export_dir / "top_project_candidates.csv",
                export_dir / "recommended_expansion_candidates.xlsx",
                export_dir / "recommended_expansion_candidates.csv",
            ],
        ),
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the CraneGenius MVP pipeline end to end.")
    parser.add_argument("--db", default=str(DEFAULT_DB), help="SQLite database path")
    parser.add_argument("--export-dir", default=str(DEFAULT_EXPORT_DIR), help="Operator export directory")
    parser.add_argument("--json-output-dir", default=str(DEFAULT_JSON_EXPORT_DIR), help="Static JSON output directory")
    parser.add_argument("--runs-dir", default=str(DEFAULT_RUNS_DIR), help="Directory for run summaries and logs")
    args = parser.parse_args()

    db_path = Path(args.db).expanduser()
    export_dir = Path(args.export_dir).expanduser()
    json_output_dir = Path(args.json_output_dir).expanduser()
    runs_dir = Path(args.runs_dir).expanduser()
    jobs_path = DEFAULT_JOBS_OUTPUT
    permits_path = DEFAULT_PERMITS_OUTPUT

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir = runs_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    log_path = run_dir / "pipeline.log"
    configure_logging(log_path)
    log = logging.getLogger("cranegenius.mvp_pipeline")

    stages = build_stages(db_path, jobs_path, permits_path, export_dir, json_output_dir)
    stage_results: List[Dict[str, object]] = []
    overall_status = "success"

    for stage in stages:
        result = run_stage(stage, log, db_path, jobs_path, permits_path, export_dir, json_output_dir)
        stage_results.append(result)
        if result["status"] != "success":
            overall_status = "failed"
            break

    final_metrics = compute_stage_metrics("build_project_intelligence", db_path, jobs_path, permits_path, export_dir, json_output_dir)
    final_metrics.update(compute_stage_metrics("export_project_intelligence", db_path, jobs_path, permits_path, export_dir, json_output_dir))
    final_metrics.update(
        {
            "status": overall_status,
            "db_path": str(db_path),
            "export_dir": str(export_dir),
            "operator_exports_hint": str(export_dir),
        }
    )

    machine_summary_path = run_dir / "run_summary.json"
    final_metrics["machine_summary_path"] = str(machine_summary_path)
    machine_summary = {
        "run_id": run_id,
        "generated_at": utc_now(),
        "repo_root": str(ROOT),
        "status": overall_status,
        "db_path": str(db_path),
        "jobs_input": str(jobs_path),
        "permits_input": str(permits_path),
        "export_dir": str(export_dir),
        "json_output_dir": str(json_output_dir),
        "stages": stage_results,
        "final_metrics": final_metrics,
    }
    machine_summary_path.write_text(json.dumps(machine_summary, indent=2), encoding="utf-8")

    operator_summary_path = run_dir / "operator_summary.md"
    operator_summary_path.write_text(summarize_operator_view(stage_results, final_metrics), encoding="utf-8")

    latest_summary = runs_dir / "latest_run_summary.json"
    latest_operator = runs_dir / "latest_operator_summary.md"
    latest_log = runs_dir / "latest_pipeline.log"
    latest_summary.write_text(json.dumps(machine_summary, indent=2), encoding="utf-8")
    latest_operator.write_text(operator_summary_path.read_text(encoding="utf-8"), encoding="utf-8")
    latest_log.write_text(log_path.read_text(encoding="utf-8"), encoding="utf-8")

    if overall_status != "success":
        logging.error("MVP pipeline failed. See %s", machine_summary_path)
        return 1

    logging.info("MVP pipeline completed successfully. Summary: %s", machine_summary_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
