from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from scripts.export_directory_json import build_directory_payload


class TestDirectoryExport(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.db_path = Path(self.tmpdir.name) / "directory.db"
        self.seed_path = Path(self.tmpdir.name) / "suppliers_seed.json"
        self._init_db()
        self._write_seed()

    def _init_db(self) -> None:
        conn = sqlite3.connect(self.db_path)
        conn.execute(
            """
            CREATE TABLE companies (
                company_id INTEGER PRIMARY KEY,
                company_name TEXT,
                location_city TEXT,
                location_state TEXT,
                target_score REAL,
                priority_reason TEXT,
                updated_at TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE opportunity_company_matches (
                opportunity_company_match_id INTEGER PRIMARY KEY,
                opportunity_feed_id INTEGER,
                company_id INTEGER,
                match_score REAL,
                match_reason TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            "INSERT INTO companies(company_id, company_name, location_city, location_state, target_score, priority_reason, updated_at) VALUES (1, 'Maxim Crane Works', 'Chicago', 'IL', 88.5, 'national heavy lift', '2026-03-12T10:00:00Z')"
        )
        conn.executemany(
            "INSERT INTO opportunity_company_matches(opportunity_company_match_id, opportunity_feed_id, company_id, match_score, match_reason) VALUES (?, ?, ?, ?, ?)",
            [
                (1, 10, 1, 0.9, "tower crane support"),
                (2, 11, 1, 0.86, "permits show crane scope"),
            ],
        )
        conn.commit()
        conn.close()

    def _write_seed(self) -> None:
        payload = {
            "version": "v0.1",
            "suppliers": [
                {
                    "supplier_id": "sup-001",
                    "company_name": "Maxim Crane Works",
                    "company_type": "crane_rental_company",
                    "service_regions": ["national"],
                    "equipment_categories": ["mobile_cranes"],
                    "active_status": True,
                },
                {
                    "supplier_id": "sup-002",
                    "company_name": "Fictional Mini Crane Co",
                    "company_type": "mini_crane_operator",
                    "service_regions": ["west"],
                    "equipment_categories": ["spider_cranes"],
                },
            ],
        }
        self.seed_path.write_text(json.dumps(payload), encoding="utf-8")

    def test_directory_payload_enriches_metrics(self) -> None:
        payload = build_directory_payload(self.db_path, self.seed_path)
        self.assertEqual(payload["count"], 2)
        matched = next(s for s in payload["suppliers"] if s["supplier_id"] == "sup-001")
        metrics = matched["metrics"]
        self.assertEqual(metrics["matched_company_id"], 1)
        self.assertEqual(metrics["opportunity_matches"], 2)
        self.assertEqual(metrics["target_score"], 88.5)
        self.assertEqual(matched["city"], "Chicago")
        self.assertEqual(matched["state_or_province"], "IL")

        unmatched = next(s for s in payload["suppliers"] if s["supplier_id"] == "sup-002")
        self.assertIsNone(unmatched["metrics"]["matched_company_id"])
        self.assertEqual(unmatched["metrics"]["opportunity_matches"], 0)


if __name__ == "__main__":
    unittest.main()
