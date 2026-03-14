#!/usr/bin/env python3
"""Lightweight health check for the autonomy cycle orchestrator."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
CYCLE_SCRIPT = REPO_ROOT / "scripts" / "autonomy" / "run_autonomy_cycle.py"
LATEST_SELECTED = REPO_ROOT / "runs" / "mvp_pipeline" / "latest_selected_task.json"
REGISTRY_PATH = REPO_ROOT / "autonomy" / "config" / "signal_registry.yaml"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def load_signals() -> List[Dict[str, Any]]:
    if not REGISTRY_PATH.exists():
        raise FileNotFoundError(f"Signal registry missing at {REGISTRY_PATH}")
    payload = yaml.safe_load(REGISTRY_PATH.read_text(encoding="utf-8")) or {}
    signals = payload.get("signals")
    return signals if isinstance(signals, list) else []


def ensure_candidate_alignment(signals: List[Dict[str, Any]], selected: Dict[str, Any]) -> None:
    from scripts.autonomy import run_autonomy_cycle as cycle

    queue = cycle.build_candidate_queue(signals)
    if not queue:
        raise RuntimeError("No candidate signals available; registry may be empty.")

    task = selected.get("task")
    if not task:
        raise RuntimeError("Cycle run succeeded but no task was emitted.")

    expected_id = queue[0].signal.get("id")
    actual_id = task.get("signal_id")
    if expected_id != actual_id:
        raise RuntimeError(f"Selected task {actual_id} did not match queue head {expected_id}.")


def run_cycle(dry_run: bool, force: bool) -> subprocess.CompletedProcess[str]:
    cmd = [sys.executable, str(CYCLE_SCRIPT)]
    if dry_run:
        cmd.append("--dry-run")
    if force:
        cmd.append("--force")
    return subprocess.run(cmd, cwd=str(REPO_ROOT), capture_output=True, text=True)


def load_selected_task() -> Dict[str, Any]:
    if not LATEST_SELECTED.exists():
        raise FileNotFoundError(f"{LATEST_SELECTED} missing after cycle run.")
    return json.loads(LATEST_SELECTED.read_text(encoding="utf-8"))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate run_autonomy_cycle task selection deterministically.")
    parser.add_argument("--no-dry-run", action="store_true", help="Invoke the cycle without --dry-run.")
    parser.add_argument("--no-force", action="store_true", help="Do not override an existing lock file.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    signals = load_signals()
    result = run_cycle(dry_run=not args.no_dry_run, force=not args.no_force)
    if result.returncode != 0:
        raise SystemExit(f"run_autonomy_cycle.py exited with {result.returncode}: {result.stderr.strip()}")
    selected = load_selected_task()
    ensure_candidate_alignment(signals, selected)
    task = selected.get("task", {})
    print(
        f"Health check passed. Selected {task.get('signal_id')} "
        f"({task.get('task_type')}) priority={task.get('priority')}."
    )


if __name__ == "__main__":
    main()
