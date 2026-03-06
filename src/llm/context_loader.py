from __future__ import annotations
import json, os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

REPO_ROOT = Path(__file__).parent.parent.parent
CONTEXT_FILE = REPO_ROOT / "context" / "project_context.md"
RUNS_DIR = REPO_ROOT / "runs"

def load_project_context() -> str:
    if not CONTEXT_FILE.exists(): return ""
    return CONTEXT_FILE.read_text(encoding="utf-8")

def load_last_run_state() -> Optional[Dict[str, Any]]:
    if not RUNS_DIR.exists(): return None
    run_files = sorted(RUNS_DIR.glob("*/run_state.json"), reverse=True)
    if not run_files: return None
    try: return json.loads(run_files[0].read_text(encoding="utf-8"))
    except Exception: return None

def save_run_state(state: Dict[str, Any]) -> Path:
    today = datetime.now().strftime("%Y-%m-%d")
    run_dir = RUNS_DIR / today
    run_dir.mkdir(parents=True, exist_ok=True)
    state_file = run_dir / "run_state.json"
    existing = {}
    if state_file.exists():
        try: existing = json.loads(state_file.read_text(encoding="utf-8"))
        except Exception: pass
    merged = {**existing, **state, "updated_at": datetime.now().isoformat()}
    state_file.write_text(json.dumps(merged, indent=2), encoding="utf-8")
    return state_file

def build_system_prompt(extra_context: str = "") -> str:
    parts = ["You are the CraneGenius AI pipeline assistant.", "You have full context about the CraneGenius project below.", "Always return only valid JSON when asked to produce structured output.", "\n## PROJECT CONTEXT\n", load_project_context()]
    last_run = load_last_run_state()
    if last_run:
        parts.append("\n## LAST RUN STATE\n")
        parts.append(json.dumps(last_run, indent=2))
    if extra_context: parts.append(f"\n## ADDITIONAL CONTEXT\n{extra_context}")
    return "\n".join(parts)

def inject_context(messages: List[Dict[str, str]], extra_context: str = "") -> List[Dict[str, str]]:
    system_prompt = build_system_prompt(extra_context)
    if messages and messages[0]["role"] == "system":
        merged = system_prompt + "\n\n" + messages[0]["content"]
        return [{"role": "system", "content": merged}] + messages[1:]
    return [{"role": "system", "content": system_prompt}] + messages

def update_context_note(note: str) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    entry = f"\n## Note [{timestamp}]\n{note}\n"
    with open(CONTEXT_FILE, "a", encoding="utf-8") as f: f.write(entry)
    print(f"  [context] Note saved to project_context.md")
