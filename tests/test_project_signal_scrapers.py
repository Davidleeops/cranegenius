from __future__ import annotations

import sys
import unittest
from contextlib import ExitStack
from pathlib import Path
from typing import Dict
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.append(str(SCRIPTS_DIR))
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

from scripts.signal_construction_bids import collect_procurement_rfps
from scripts.signal_infrastructure_projects import collect_infrastructure_announcements
from scripts.signal_prebid_attendance import (
    collect_prebid_attendance_signals,
    dedupe_attendees,
    parse_nyc_attendees,
)
from scripts.scrape_permits_public import collect_project_signals, is_crane_candidate, normalize_row
from contact_intelligence.scripts.build_project_intelligence import compute_multi_scores, SCORE_VERSION


class TestInfrastructureScraper(unittest.TestCase):
    def test_collect_infrastructure_normalizes_rows(self) -> None:
        sample_payload = {
            "results": [
                {
                    "document_number": "2025-12345",
                    "title": "DOT Funds New Hudson River Bridge",
                    "abstract": "Award announces design-build partner selection for a major span.",
                    "publication_date": "2025-02-10",
                    "html_url": "https://www.federalregister.gov/example",
                    "locations": ["New York, NY"],
                }
            ]
        }
        rows, stats = collect_infrastructure_announcements(limit=1, fetcher=lambda url: (sample_payload, [{"status": "ok"}]))
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["source"], "dot_infrastructure_announcements")
        self.assertEqual(row["project_stage"], "announcement")
        self.assertEqual(row["estimated_lift_activity"], "high")
        self.assertGreater(row["confidence_score"], 0.7)
        self.assertEqual(stats["records_returned"], 1)


class TestProcurementScraper(unittest.TestCase):
    def test_collect_procurement_rfps_maps_notice_fields(self) -> None:
        sample_payload = [
            {
                "title": "Tower Crane Services IDIQ",
                "section_details": "City seeks tower crane services for civic center expansion.",
                "start_date": "2025-01-05",
                "end_date": "2025-02-01",
                "display_type": "Request for Proposal",
                "request_id": "RFP-99",
                "url": "https://cityrecord.nyc.gov/rfp-99",
            }
        ]
        def fake_fetch(url: str, **_: object):
            if "dg92-zbpx" in url:
                return sample_payload, [{"status": "ok"}]
            return [], [{"status": "ok"}]

        rows, stats = collect_procurement_rfps(limit=1, fetcher=fake_fetch)
        nyc_rows = [row for row in rows if row["source"] == "nyc_city_record_procurement"]
        self.assertEqual(len(nyc_rows), 1)
        row = nyc_rows[0]
        self.assertEqual(row["source"], "nyc_city_record_procurement")
        self.assertEqual(row["project_stage"], "procurement")
        self.assertEqual(row["state"], "NY")
        self.assertIn("tower crane", row["description"].lower())
        self.assertGreater(row["confidence_score"], 0.6)
        nyc_stats = [s for s in stats if s["source"] == "nyc_city_record_procurement"][0]
        self.assertEqual(nyc_stats["records_returned"], 1)
        self.assertEqual(len(stats), 3)


class TestPermitNoiseFilter(unittest.TestCase):
    def setUp(self) -> None:
        self.source = {"name": "chicago_permits", "url": "https://example.com", "city": "Chicago", "state": "IL"}

    def test_noise_keywords_are_filtered(self) -> None:
        fence_row = {
            "work_description": "ERECT A NEW 6' WOODEN FENCE WITH STEEL POSTS.",
            "address": "100 W MAIN ST",
            "issue_date": "2026-03-10",
            "permit_": "FNC-1",
        }
        normalized = normalize_row(fence_row, self.source)
        self.assertFalse(normalized["is_opportunity_candidate"])
        self.assertFalse(is_crane_candidate("ERECT NEW WOOD FENCE"))

    def test_legitimate_crane_activity_is_preserved(self) -> None:
        crane_row = {
            "work_description": "INSTALL FREE STANDING LIEBHERR 316 EC-H 12 LITRONIC TOWER CRANE.",
            "address": "233 W CRANE AVE",
            "issue_date": "2026-03-10",
            "permit_": "CRN-9",
        }
        normalized = normalize_row(crane_row, self.source)
        self.assertTrue(normalized["is_opportunity_candidate"])
        self.assertTrue(is_crane_candidate(crane_row["work_description"]))


class TestPrebidAttendanceScraper(unittest.TestCase):
    def test_collect_prebid_attendance_normalizes_rows(self) -> None:
        rows, stats = collect_prebid_attendance_signals(limit=2)
        self.assertEqual(len(rows), 2)
        self.assertEqual(len(stats), 2)
        first = rows[0]
        self.assertEqual(first["signal_family"], "prebid_attendance")
        self.assertEqual(first["agency"], "TxDOT Austin District")
        self.assertGreaterEqual(first["attendee_count"], 3)
        self.assertTrue(first["meeting_date"].startswith("2026-03-20"))
        self.assertTrue(any(att["trade_role"] == "heavy_lift_partner" for att in first["attendees"]))

    def test_parse_nyc_attendees_handles_multiline_and_dedupes(self) -> None:
        attendees = parse_nyc_attendees(["Empire Cranes\\n - Marco Silva (Heavy Lift)", "Empire Cranes - Marco Silva (Heavy Lift)"])
        self.assertEqual(len(attendees), 2)
        cleaned = dedupe_attendees(attendees)
        self.assertEqual(len(cleaned), 1)
        self.assertEqual(cleaned[0]["company"], "Empire Cranes")
        self.assertEqual(cleaned[0]["trade_role"], "heavy_lift_partner")


class TestProjectSignalAggregator(unittest.TestCase):
    def test_collect_project_signals_flattens_stats(self) -> None:
        def dict_stat(name: str) -> Dict[str, object]:
            return {
                "source": name,
                "signal_type": name,
                "records_returned": 0,
                "empty_feed": True,
                "schema_drift_records": 0,
                "attempts": [],
                "error_categories": [],
            }

        procurement_stats = [
            dict_stat("proc_nyc"),
            dict_stat("proc_sam"),
        ]
        heavy_stats = [
            dict_stat("heavy_state"),
            dict_stat("heavy_corridor"),
        ]
        federal_stats = [
            dict_stat("infra_awards"),
            dict_stat("mega_awards"),
        ]

        with ExitStack() as stack:
            stack.enter_context(patch("scripts.scrape_permits_public.collect_infrastructure_announcements", return_value=([], dict_stat("infra"))))
            stack.enter_context(patch("scripts.scrape_permits_public.collect_txdot_capital_plans", return_value=([], dict_stat("txdot_capital"))))
            stack.enter_context(patch("scripts.scrape_permits_public.collect_caltrans_capital_plans", return_value=([], dict_stat("caltrans_capital"))))
            stack.enter_context(patch("scripts.scrape_permits_public.collect_fairfax_capital_plan_projects", return_value=([], dict_stat("fairfax_capital"))))
            stack.enter_context(patch("scripts.scrape_permits_public.collect_manatee_capital_plan_projects", return_value=([], dict_stat("manatee_capital"))))
            stack.enter_context(patch("scripts.scrape_permits_public.collect_siouxfalls_capital_plan_projects", return_value=([], dict_stat("siouxfalls_capital"))))
            stack.enter_context(patch("scripts.scrape_permits_public.collect_tdot_stip_projects", return_value=([], dict_stat("tdot_stip"))))
            stack.enter_context(patch("scripts.scrape_permits_public.collect_zoning_filings", return_value=([], dict_stat("zoning_filings"))))
            stack.enter_context(patch("scripts.scrape_permits_public.collect_federal_earmark_awards", return_value=([], federal_stats)))
            stack.enter_context(patch("scripts.scrape_permits_public.collect_procurement_rfps", return_value=([], procurement_stats)))
            stack.enter_context(patch("scripts.scrape_permits_public.collect_lift_permits", return_value=([], dict_stat("lift_permits"))))
            stack.enter_context(patch("scripts.scrape_permits_public.collect_equipment_rental_signals", return_value=([], dict_stat("rental_signals"))))
            stack.enter_context(patch("scripts.scrape_permits_public.collect_subcontractor_registrations", return_value=([], dict_stat("subcontractor"))))
            stack.enter_context(patch("scripts.scrape_permits_public.collect_utility_expansion", return_value=([], dict_stat("utility_expansion"))))
            stack.enter_context(patch("scripts.scrape_permits_public.collect_utility_infrastructure", return_value=([], dict_stat("utility_infrastructure"))))
            stack.enter_context(patch("scripts.scrape_permits_public.collect_utility_irp_signals", return_value=([], dict_stat("utility_irp"))))
            stack.enter_context(patch("scripts.scrape_permits_public.collect_oversize_loads", return_value=([], heavy_stats)))
            stack.enter_context(patch("scripts.scrape_permits_public.collect_corporate_capex_signals", return_value=([], dict_stat("corporate_capex"))))
            stack.enter_context(patch("scripts.scrape_permits_public.collect_prebid_attendance_signals", return_value=([], dict_stat("prebid_attendance"))))
            stack.enter_context(patch("scripts.scrape_permits_public.collect_site_plan_reviews", return_value=([], dict_stat("data_center_site_plans"))))
            stack.enter_context(patch("scripts.scrape_permits_public.collect_bess_procurements", return_value=([], dict_stat("battery_storage_procurements"))))

            rows, stats = collect_project_signals(limit=2)

        self.assertEqual(rows, [])
        self.assertEqual(len(stats), 24)
        sources = {stat["source"] for stat in stats}
        self.assertIn("proc_nyc", sources)
        self.assertIn("proc_sam", sources)
        self.assertIn("heavy_state", sources)
        self.assertIn("heavy_corridor", sources)
        self.assertIn("infra_awards", sources)
        self.assertIn("mega_awards", sources)
        self.assertIn("zoning_filings", sources)
        self.assertIn("corporate_capex", sources)
        self.assertIn("utility_irp", sources)
        self.assertIn("prebid_attendance", sources)
        self.assertIn("data_center_site_plans", sources)
        self.assertIn("battery_storage_procurements", sources)


class TestMultiScoreModel(unittest.TestCase):
    def test_compute_multi_scores_weighting(self) -> None:
        candidate = {
            "confidence_score": 70.0,
            "demand_score": 80.0,
            "timing_score": 65.0,
            "crane_relevance_score": 90.0,
            "mini_crane_fit_score": 40.0,
            "monetization_score": 85.0,
            "estimated_spend_proxy": 90.0,
        }
        scores = compute_multi_scores(candidate)
        self.assertAlmostEqual(scores["project_reality_score"], 75.0)
        self.assertAlmostEqual(scores["equipment_intensity_score"], 70.0)
        self.assertAlmostEqual(scores["buyer_value_score"], 87.5)
        self.assertAlmostEqual(scores["overall_priority_score"], 74.88, places=2)
        self.assertEqual(scores["score_version"], SCORE_VERSION)


if __name__ == "__main__":
    unittest.main()
