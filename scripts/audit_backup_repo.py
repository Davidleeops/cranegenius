#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Set

IGNORE_DIRS = {".git", "__pycache__"}
IGNORE_FILES = {".DS_Store"}
IGNORE_SUFFIXES = {".pyc"}


def should_ignore(rel_path: Path) -> bool:
    parts = rel_path.parts
    if any(part in IGNORE_DIRS for part in parts):
        return True
    name = rel_path.name
    if name in IGNORE_FILES:
        return True
    if rel_path.suffix in IGNORE_SUFFIXES:
        return True
    return False


def file_hash(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def build_file_map(root: Path) -> Dict[Path, Path]:
    file_map: Dict[Path, Path] = {}
    for p in root.rglob("*"):
        if not p.is_file():
            continue
        rel = p.relative_to(root)
        if should_ignore(rel):
            continue
        file_map[rel] = p
    return file_map


def sorted_lines(paths: Iterable[Path]) -> List[str]:
    return [str(p) for p in sorted(paths)]


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit differences between live and backup repos (read-only).")
    parser.add_argument("--live", default="/Users/lemueldavidleejr/Downloads/cranegenius_repo", help="Canonical live repo path")
    parser.add_argument("--backup", default="/Users/lemueldavidleejr/Desktop/cranegenius/cranegenius_repo_backup_unreviewed", help="Backup repo path")
    parser.add_argument("--report", default="runs/repo_backup_audit.md", help="Report path relative to live repo unless absolute")
    args = parser.parse_args()

    live = Path(args.live).expanduser().resolve()
    backup = Path(args.backup).expanduser().resolve()

    if not live.exists() or not live.is_dir():
        raise SystemExit(f"Live repo not found: {live}")
    if not backup.exists() or not backup.is_dir():
        raise SystemExit(f"Backup repo not found: {backup}")

    report_path = Path(args.report)
    if not report_path.is_absolute():
        report_path = (live / report_path).resolve()

    live_map = build_file_map(live)
    backup_map = build_file_map(backup)

    live_set: Set[Path] = set(live_map.keys())
    backup_set: Set[Path] = set(backup_map.keys())

    only_in_backup = backup_set - live_set
    only_in_live = live_set - backup_set

    common = sorted(live_set & backup_set)
    differ: List[Path] = []
    for rel in common:
        lp = live_map[rel]
        bp = backup_map[rel]
        try:
            if lp.stat().st_size != bp.stat().st_size or file_hash(lp) != file_hash(bp):
                differ.append(rel)
        except FileNotFoundError:
            differ.append(rel)

    lines: List[str] = []
    lines.append("# Repo Backup Audit")
    lines.append("")
    lines.append(f"- Generated: {datetime.now().isoformat()}")
    lines.append(f"- Live repo: `{live}`")
    lines.append(f"- Backup repo: `{backup}`")
    lines.append("")
    lines.append("## Summary")
    lines.append(f"- Only in backup: **{len(only_in_backup)}**")
    lines.append(f"- Only in live: **{len(only_in_live)}**")
    lines.append(f"- Differing files: **{len(differ)}**")
    lines.append("")

    lines.append("## Files Only In Backup")
    if only_in_backup:
        lines.extend(f"- `{p}`" for p in sorted_lines(only_in_backup))
    else:
        lines.append("- (none)")
    lines.append("")

    lines.append("## Files Only In Live")
    if only_in_live:
        lines.extend(f"- `{p}`" for p in sorted_lines(only_in_live))
    else:
        lines.append("- (none)")
    lines.append("")

    lines.append("## Files That Differ")
    if differ:
        lines.extend(f"- `{p}`" for p in sorted_lines(differ))
    else:
        lines.append("- (none)")
    lines.append("")

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("\n".join(lines), encoding="utf-8")

    print(f"Live repo: {live}")
    print(f"Backup repo: {backup}")
    print(f"Only in backup: {len(only_in_backup)}")
    print(f"Only in live: {len(only_in_live)}")
    print(f"Differing files: {len(differ)}")
    print(f"Report written: {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
