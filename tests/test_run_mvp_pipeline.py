from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from scripts.run_mvp_pipeline import StageDefinition, build_stages, compute_stage_metrics, validate_stage_outputs


class TestMvpPipelineHelpers(unittest.TestCase):
    def test_validate_stage_outputs_marks_required_and_optional(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            required = root / "required.json"
            optional = root / "optional.json"
            required.write_text("{}", encoding="utf-8")
            stage = StageDefinition(
                name="example",
                command=["python3", "example.py"],
                required_outputs=[required],
                optional_outputs=[optional],
            )

            result = validate_stage_outputs(stage)
            self.assertEqual(len(result), 2)
            self.assertTrue(result[0]["exists"])
            self.assertTrue(result[0]["required"])
            self.assertFalse(result[1]["exists"])
            self.assertFalse(result[1]["required"])

    def test_compute_stage_metrics_reads_json_payloads(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            jobs = root / "jobs.json"
            permits = root / "permits.json"
            export_dir = root / "exports"
            json_dir = root / "json"
            static_dir = json_dir / "static_exports"
            export_dir.mkdir(parents=True)
            static_dir.mkdir(parents=True)

            jobs.write_text(json.dumps({"count": 5, "raw_rows": 9, "used_previous_output": False, "used_seed_catalog": True}), encoding="utf-8")
            permits.write_text(json.dumps({"opportunity_candidates": 3, "rows_fetched": 7, "used_previous_output": True}), encoding="utf-8")
            (static_dir / "top_project_candidates.json").write_text(json.dumps([{"id": 1}, {"id": 2}]), encoding="utf-8")
            (static_dir / "recommended_expansion_candidates.json").write_text(json.dumps([{"id": 3}]), encoding="utf-8")
            (export_dir / "top_project_candidates.csv").write_text("a,b\n", encoding="utf-8")

            job_metrics = compute_stage_metrics("scrape_jobs", root / "db.sqlite", jobs, permits, export_dir, json_dir)
            permit_metrics = compute_stage_metrics("scrape_permits", root / "db.sqlite", jobs, permits, export_dir, json_dir)
            export_metrics = compute_stage_metrics("export_project_intelligence", root / "db.sqlite", jobs, permits, export_dir, json_dir)

            self.assertEqual(job_metrics["jobs_count"], 5)
            self.assertTrue(job_metrics["used_seed_catalog"])
            self.assertEqual(permit_metrics["permit_candidate_rows"], 3)
            self.assertTrue(permit_metrics["used_previous_output"])
            self.assertEqual(export_metrics["top_project_candidates_records"], 2)
            self.assertEqual(export_metrics["recommended_expansion_candidates_records"], 1)

    def test_build_stages_declares_explicit_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            db = root / "ci.db"
            jobs = root / "jobs.json"
            permits = root / "permits.json"
            export_dir = root / "exports"
            json_dir = root / "json"

            stages = build_stages(db, jobs, permits, export_dir, json_dir)
            self.assertEqual(stages[0].name, "init_db")
            self.assertEqual(stages[-1].name, "export_project_intelligence")
            self.assertTrue(stages[-1].required_outputs)
            self.assertTrue(any("top_project_candidates.json" in str(path) for path in stages[-1].required_outputs))


if __name__ == "__main__":
    unittest.main()
