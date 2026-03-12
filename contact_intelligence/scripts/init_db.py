#!/usr/bin/env python3
"""
contact_intelligence/scripts/init_db.py
Initialize the CraneGenius contact intelligence SQLite database.

Usage (from repo root):
    python3 contact_intelligence/scripts/init_db.py
    python3 contact_intelligence/scripts/init_db.py --db /custom/path/ci.db
"""

import os
import sys
import sqlite3
import argparse
from pathlib import Path

DEFAULT_DB = os.path.expanduser("~/data_runtime/cranegenius_ci.db")
SCHEMA_DIR = Path(__file__).parent.parent / "schema"


def get_db_path(arg=None):
    return arg or os.environ.get("CRANEGENIUS_CI_DB", DEFAULT_DB)


def init_db(db_path: str):
    db = Path(db_path)
    db.parent.mkdir(parents=True, exist_ok=True)
    for sub in ["imports", "exports", "backups", "logs", "seeds"]:
        (db.parent / sub).mkdir(exist_ok=True)

    print(f"[init_db] Initializing: {db}")
    conn = sqlite3.connect(str(db))
    c = conn.cursor()

    for sql_file in ["create_tables.sql", "indexes.sql", "seed_sectors.sql"]:
        p = SCHEMA_DIR / sql_file
        if not p.exists():
            print(f"[init_db] ERROR: missing {p}")
            sys.exit(1)
        print(f"[init_db]  Applying {sql_file}...")
        c.executescript(p.read_text())

    conn.commit()
    conn.close()
    print(f"[init_db] ✓ Done. Runtime folders at: {db.parent}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--db", default=None)
    args = p.parse_args()
    init_db(get_db_path(args.db))
