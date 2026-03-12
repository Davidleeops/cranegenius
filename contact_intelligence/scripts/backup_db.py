#!/usr/bin/env python3
"""
contact_intelligence/scripts/backup_db.py
Create timestamped backup of the SQLite database.

Usage (from repo root):
    python3 contact_intelligence/scripts/backup_db.py
    python3 contact_intelligence/scripts/backup_db.py --keep 14
"""

import os, sys, sqlite3, argparse
from pathlib import Path
from datetime import datetime

DEFAULT_DB     = os.path.expanduser("~/data_runtime/cranegenius_ci.db")
DEFAULT_BACKUP = os.path.expanduser("~/data_runtime/backups")
DEFAULT_KEEP   = 7


def get_db(arg=None):
    return arg or os.environ.get("CRANEGENIUS_CI_DB", DEFAULT_DB)


def backup(db_path, backup_dir, keep):
    db = Path(db_path)
    if not db.exists(): sys.exit(f"[backup] DB not found: {db}")
    Path(backup_dir).mkdir(parents=True, exist_ok=True)
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest = Path(backup_dir) / f"cranegenius_ci_{ts}.db"
    src  = sqlite3.connect(str(db)); dst = sqlite3.connect(str(dest))
    src.backup(dst); dst.close(); src.close()
    mb = dest.stat().st_size / 1024**2
    print(f"[backup] ✓ {dest.name} ({mb:.2f} MB)")
    # Prune
    all_bk = sorted(Path(backup_dir).glob("cranegenius_ci_*.db"),
                    key=lambda f: f.stat().st_mtime, reverse=True)
    for old in all_bk[keep:]:
        old.unlink(); print(f"[backup]   Removed: {old.name}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--db",     default=None)
    p.add_argument("--output", default=DEFAULT_BACKUP)
    p.add_argument("--keep",   default=DEFAULT_KEEP, type=int)
    a = p.parse_args()
    backup(get_db(a.db), a.output, a.keep)
