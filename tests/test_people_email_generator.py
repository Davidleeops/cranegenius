from __future__ import annotations

import unittest

import pandas as pd

from src.people_email_generator import generate_email_candidates_for_people


class TestPeopleEmailGenerator(unittest.TestCase):
    def test_normal_two_part_name_generates_expected_patterns(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "first_name": "John",
                    "last_name": "Martinez",
                    "contractor_name_normalized": "acme construction",
                    "contractor_domain": "acmeconstruction.com",
                    "title": "Project Manager",
                    "title_confirmed": True,
                    "source_url": "https://acmeconstruction.com/team",
                    "is_role_inbox": False,
                }
            ]
        )
        out = generate_email_candidates_for_people(df)
        emails = set(out["email"].tolist())
        self.assertEqual(len(out), 2)
        self.assertIn("john.martinez@acmeconstruction.com", emails)
        self.assertIn("jmartinez@acmeconstruction.com", emails)

    def test_middle_initial_in_full_name_not_used_in_patterns(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "full_name": "John Q. Martinez",
                    "contractor_name_normalized": "acme construction",
                    "contractor_domain": "acmeconstruction.com",
                    "title": "Estimator",
                    "title_confirmed": True,
                    "source_url": "https://acmeconstruction.com/leadership",
                    "is_role_inbox": False,
                }
            ]
        )
        out = generate_email_candidates_for_people(df)
        emails = set(out["email"].tolist())
        self.assertIn("john.martinez@acmeconstruction.com", emails)
        self.assertNotIn("john.q.martinez@acmeconstruction.com", emails)
        self.assertNotIn("jqmartinez@acmeconstruction.com", emails)

    def test_duplicate_pattern_dedupes_across_rows(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "first_name": "John",
                    "last_name": "Martinez",
                    "contractor_name_normalized": "acme construction",
                    "contractor_domain": "acmeconstruction.com",
                },
                {
                    "first_name": "John",
                    "last_name": "Martinez",
                    "contractor_name_normalized": "acme construction",
                    "contractor_domain": "acmeconstruction.com",
                },
            ]
        )
        out = generate_email_candidates_for_people(df)
        self.assertEqual(len(out), len(set(out["email"].tolist())))
        self.assertEqual(len(out), 2)


    def test_explicit_max_patterns_keeps_extended_coverage(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "first_name": "John",
                    "last_name": "Martinez",
                    "contractor_name_normalized": "acme construction",
                    "contractor_domain": "acmeconstruction.com",
                }
            ]
        )
        out = generate_email_candidates_for_people(df, max_patterns_per_person=8)
        emails = set(out["email"].tolist())
        self.assertGreaterEqual(len(out), 7)
        self.assertIn("john_martinez@acmeconstruction.com", emails)
        self.assertIn("martinez.john@acmeconstruction.com", emails)


if __name__ == "__main__":
    unittest.main()
