#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

ROOT = Path(__file__).resolve().parents[1]
DOCS_DIR = ROOT / "docs_learning"
UPLOAD_BUNDLE_DIR = DOCS_DIR / "upload_bundle"

BUILD_LOG_PATH = DOCS_DIR / "build_log.md"
LEARNING_SUMMARY_PATH = DOCS_DIR / "learning_summary.md"
PODCAST_SCRIPT_PATH = DOCS_DIR / "podcast_script.md"
HANDOFF_PATH = DOCS_DIR / "claude_browser_handoff.md"

DEFAULT_PIPELINE_CMD = "python3 -m src.monday_people_pipeline --mode run"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ensure_dirs() -> None:
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    UPLOAD_BUNDLE_DIR.mkdir(parents=True, exist_ok=True)


def _run_pipeline(command: str) -> Dict[str, str]:
    proc = subprocess.run(
        command,
        shell=True,
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    combined = (proc.stdout or "") + ("\n" if proc.stdout and proc.stderr else "") + (proc.stderr or "")
    return {
        "command": command,
        "exit_code": str(proc.returncode),
        "stdout": proc.stdout or "",
        "stderr": proc.stderr or "",
        "combined": combined.strip(),
    }


def _write_build_log(run_meta: Dict[str, str]) -> None:
    content = f"""# Build Log

## Run Metadata
- Timestamp (UTC): { _now_iso() }
- Working directory: `{ROOT}`
- Command: `{run_meta["command"]}`
- Exit code: `{run_meta["exit_code"]}`

## Terminal Output
```text
{run_meta["combined"] or "(no output captured)"}
```
"""
    BUILD_LOG_PATH.write_text(content, encoding="utf-8")


def _load_json(path: Path) -> Dict[str, object]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _candidate_artifacts() -> List[Path]:
    data = ROOT / "data"
    ordered = [
        data / "monday_people_pipeline_qa.json",
        data / "monday_company_domains.csv",
        data / "monday_people_found.csv",
        data / "monday_all_email_candidates.csv",
        data / "monday_verified_valid.csv",
        data / "monday_verified_catchall.csv",
        data / "monday_verified_invalid.csv",
        data / "monday_plusvibes_combined.csv",
        data / "qa_report.json",
    ]
    return [p for p in ordered if p.exists()]


def _write_learning_summary(run_meta: Dict[str, str], artifacts: List[Path]) -> None:
    monday_qa = _load_json(ROOT / "data" / "monday_people_pipeline_qa.json")
    core_lines: List[str] = []
    for key in [
        "total_input_companies",
        "companies_with_domains",
        "people_found_count",
        "total_candidates_generated",
        "verified_valid_count",
        "verified_catchall_count",
        "verified_invalid_count",
    ]:
        if key in monday_qa:
            core_lines.append(f"- {key}: `{monday_qa[key]}`")
    if not core_lines:
        core_lines.append("- QA metrics not available (pipeline output missing or not generated).")

    artifact_lines = [f"- `{p.relative_to(ROOT)}`" for p in artifacts] or ["- No artifacts found."]

    content = f"""# Learning Summary

## Run Status
- Timestamp (UTC): {_now_iso()}
- Pipeline command: `{run_meta["command"]}`
- Exit code: `{run_meta["exit_code"]}`

## Key Metrics
{chr(10).join(core_lines)}

## Prepared Artifacts
{chr(10).join(artifact_lines)}

## Notes
- This package is prepared locally by Codex.
- Browser actions are intentionally deferred to Claude operator handoff.
"""
    LEARNING_SUMMARY_PATH.write_text(content, encoding="utf-8")


def _write_podcast_script(run_meta: Dict[str, str]) -> None:
    monday_qa = _load_json(ROOT / "data" / "monday_people_pipeline_qa.json")
    total_companies = monday_qa.get("total_input_companies", "N/A")
    domains = monday_qa.get("companies_with_domains", "N/A")
    candidates = monday_qa.get("total_candidates_generated", "N/A")
    valid = monday_qa.get("verified_valid_count", "N/A")

    content = f"""# Podcast Script

## Intro
Today we ran the CraneGenius learning build and prepared fresh artifacts for NotebookLM ingestion.

## Build Snapshot
- Pipeline command: {run_meta["command"]}
- Exit code: {run_meta["exit_code"]}
- Total input companies: {total_companies}
- Companies with domains: {domains}
- Total generated candidates: {candidates}
- Verified valid emails: {valid}

## Main Narrative
CraneGenius executed the local pipeline end-to-end, normalized company and people data, generated candidate outreach emails, and prepared verification-ready outputs. The latest run artifacts have been bundled for NotebookLM so a single upload action can generate updated learning context and an audio-style synthesis.

## Closing
Next step: perform the NotebookLM upload in browser and generate podcast-style output from the uploaded bundle.
"""
    PODCAST_SCRIPT_PATH.write_text(content, encoding="utf-8")


def _copy_upload_bundle(artifacts: List[Path]) -> List[Path]:
    copied: List[Path] = []
    for item in UPLOAD_BUNDLE_DIR.glob("*"):
        if item.is_file():
            item.unlink()
    always_include = [BUILD_LOG_PATH, LEARNING_SUMMARY_PATH, PODCAST_SCRIPT_PATH]
    for path in always_include + artifacts:
        if not path.exists():
            continue
        dst = UPLOAD_BUNDLE_DIR / path.name
        shutil.copy2(path, dst)
        copied.append(dst)
    return copied


def _write_handoff(copied_bundle_files: List[Path], notebook_name: str) -> None:
    file_list = "\n".join(f"- `{p}`" for p in copied_bundle_files) or "- (no files copied)"
    content = f"""# Claude Browser Handoff (NotebookLM)

## Scope
Use Claude only for browser actions. All local build/packaging work is already complete.

## Local Bundle Path
`{UPLOAD_BUNDLE_DIR}`

## Files To Upload
{file_list}

## Browser Steps (Exact)
1. Open [https://notebooklm.google.com/](https://notebooklm.google.com/).
2. Sign in with the target Google account if prompted.
3. Create or open notebook: **{notebook_name}**.
4. Choose upload/add sources.
5. Upload all files from `{UPLOAD_BUNDLE_DIR}`.
6. Wait for all source files to finish processing.
7. Generate a learning synthesis using the uploaded sources.
8. If audio/podcast generation is available, generate podcast-style audio output.
9. Confirm success/failure and list any file-level upload errors.

## Return Format For Claude
- Notebook used: `<name>`
- Files uploaded: `<count>`
- Upload result: `success` or `failure`
- Audio generated: `yes` or `no`
- Errors: `<none or details>`
"""
    HANDOFF_PATH.write_text(content, encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run pipeline locally and prepare NotebookLM learning bundle.")
    parser.add_argument("--pipeline-cmd", default=DEFAULT_PIPELINE_CMD, help="Pipeline command to execute locally.")
    parser.add_argument("--skip-pipeline", action="store_true", help="Skip pipeline run and package latest artifacts only.")
    parser.add_argument(
        "--notebook-name",
        default="CraneGenius Learning Notebook",
        help="Target NotebookLM notebook name for handoff instructions.",
    )
    args = parser.parse_args()

    _ensure_dirs()
    run_meta = {"command": args.pipeline_cmd, "exit_code": "0", "stdout": "", "stderr": "", "combined": ""}
    if not args.skip_pipeline:
        run_meta = _run_pipeline(args.pipeline_cmd)
    else:
        run_meta["combined"] = "Pipeline run skipped by --skip-pipeline. Using latest local artifacts."

    _write_build_log(run_meta)
    artifacts = _candidate_artifacts()
    _write_learning_summary(run_meta, artifacts)
    _write_podcast_script(run_meta)
    copied = _copy_upload_bundle(artifacts)
    _write_handoff(copied, notebook_name=args.notebook_name)

    print("Prepared learning artifacts.")
    print(f"Build log: {BUILD_LOG_PATH}")
    print(f"Summary: {LEARNING_SUMMARY_PATH}")
    print(f"Podcast script: {PODCAST_SCRIPT_PATH}")
    print(f"Upload bundle: {UPLOAD_BUNDLE_DIR}")
    print(f"Handoff doc: {HANDOFF_PATH}")
    return int(run_meta["exit_code"]) if not args.skip_pipeline else 0


if __name__ == "__main__":
    raise SystemExit(main())
