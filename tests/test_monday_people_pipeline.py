from __future__ import annotations

import unittest

import pandas as pd

from unittest.mock import patch

from src.monday_people_pipeline import _apply_clean_company_names, _filter_people_for_personal_generation, _person_source_confidence, _prepare_candidates_for_verification, _verify


class TestVerificationGate(unittest.TestCase):
    def test_reduces_verification_input_on_small_fixture(self) -> None:
        domains = pd.DataFrame([
            {
                "contractor_name_normalized": "acme construction",
                "contractor_domain": "acme.com",
                "domain_valid": True,
                "mx_valid": True,
            }
        ])

        candidates = pd.DataFrame([
            {
                "contractor_name_normalized": "acme construction",
                "contractor_domain": "acme.com",
                "email": "john.doe@acme.com",
                "first_name": "John",
                "last_name": "Doe",
                "email_pattern": "first.last",
                "pattern_confidence": 0.96,
                "title": "Project Manager",
                "title_confirmed": True,
                "source_url": "https://acme.com/team",
                "is_role_inbox": False,
            },
            {
                "contractor_name_normalized": "acme construction",
                "contractor_domain": "acme.com",
                "email": "jdoe@acme.com",
                "first_name": "John",
                "last_name": "Doe",
                "email_pattern": "flast",
                "pattern_confidence": 0.93,
                "title": "",
                "title_confirmed": False,
                "source_url": "",
                "is_role_inbox": False,
            },
            {
                "contractor_name_normalized": "acme construction",
                "contractor_domain": "acme.com",
                "email": "info@acme.com",
                "first_name": "",
                "last_name": "",
                "email_pattern": "role",
                "pattern_confidence": 0.99,
                "title": "",
                "title_confirmed": False,
                "source_url": "",
                "is_role_inbox": True,
            },
        ])

        out, counts = _prepare_candidates_for_verification(candidates, domains)
        self.assertEqual(len(candidates), 3)
        self.assertEqual(len(out), 1)
        self.assertEqual(counts["filtered_low_confidence"], 1)
        self.assertEqual(counts["filtered_role_inbox"], 1)
        self.assertEqual(counts["filtered_low_domain_confidence"], 0)


class TestVerificationExecutionState(unittest.TestCase):
    def _domains(self) -> pd.DataFrame:
        return pd.DataFrame([{
            "contractor_name_normalized": "acme construction",
            "contractor_domain": "acme.com",
            "domain_valid": True,
            "mx_valid": True,
        }])

    def _candidate(self) -> pd.DataFrame:
        return pd.DataFrame([{
            "contractor_name_normalized": "acme construction",
            "contractor_domain": "acme.com",
            "email": "john.doe@acme.com",
            "first_name": "John",
            "last_name": "Doe",
            "email_pattern": "first.last",
            "pattern_rank": 1,
            "pattern_confidence": 0.96,
            "title": "Project Manager",
            "title_confirmed": True,
            "source_url": "https://acme.com/team",
            "is_role_inbox": False,
        }])

    def test_missing_api_key_skips_with_reason(self) -> None:
        with patch.dict("os.environ", {}, clear=True):
            _v, _c, _i, meta = _verify(self._candidate(), self._domains())
        self.assertFalse(meta["verification_attempted"])
        self.assertTrue(meta["verification_skipped"])
        self.assertEqual(meta["skip_reason"], "missing_api_key")

    def test_no_verifier_input_skips_with_reason(self) -> None:
        weak_domains = self._domains().copy()
        weak_domains["domain_valid"] = False
        with patch.dict("os.environ", {"MILLIONVERIFIER_API_KEY": "x"}, clear=True):
            _v, _c, _i, meta = _verify(self._candidate(), weak_domains)
        self.assertFalse(meta["verification_attempted"])
        self.assertTrue(meta["verification_skipped"])
        self.assertEqual(meta["skip_reason"], "no_verifier_input")


    def test_medium_domain_confidence_proceeds_to_verifier_input(self) -> None:
        medium_domains = self._domains().copy()
        medium_domains["domain_confidence"] = "medium"
        out, counts = _prepare_candidates_for_verification(self._candidate(), medium_domains)
        self.assertEqual(len(out), 1)
        self.assertEqual(counts["filtered_low_domain_confidence"], 0)

    @patch("src.monday_people_pipeline.verify_with_millionverifier")
    def test_successful_verification_sets_attempted_true(self, mock_verify) -> None:
        mock_verify.return_value = pd.DataFrame([{
            "email": "john.doe@acme.com",
            "email_verification_status": "valid",
            "email_is_catchall": False,
            "email_quality_score": 95,
            "email_verification_provider": "millionverifier",
        }])
        with patch.dict("os.environ", {"MILLIONVERIFIER_API_KEY": "x"}, clear=True):
            _v, _c, _i, meta = _verify(self._candidate(), self._domains())
        self.assertTrue(meta["verification_attempted"])
        self.assertFalse(meta["verification_skipped"])
        self.assertEqual(meta["skip_reason"], "")
        self.assertEqual(meta["total_verified_rows_returned"], 1)
        self.assertEqual(meta["valid_count"], 1)



    def test_weak_domain_confidence_does_not_proceed_to_verifier_input(self) -> None:
        weak_domains = self._domains().copy()
        weak_domains["domain_confidence"] = "low"
        out, counts = _prepare_candidates_for_verification(self._candidate(), weak_domains)
        self.assertEqual(len(out), 0)
        self.assertGreaterEqual(counts["filtered_low_domain_confidence"], 1)

    def test_high_domain_confidence_proceeds_to_verifier_input(self) -> None:
        strong_domains = self._domains().copy()
        strong_domains["domain_confidence"] = "high"
        out, counts = _prepare_candidates_for_verification(self._candidate(), strong_domains)
        self.assertEqual(len(out), 1)
        self.assertEqual(counts["filtered_low_domain_confidence"], 0)

    def test_weak_person_without_title_or_found_email_is_blocked_pre_verification(self) -> None:
        weak_candidate = self._candidate().copy()
        weak_candidate["title_confirmed"] = False
        weak_candidate["source_url"] = ""
        weak_candidate["found_email"] = ""
        out, counts = _prepare_candidates_for_verification(weak_candidate, self._domains())
        self.assertEqual(len(out), 0)
        self.assertGreaterEqual(counts["filtered_low_confidence"], 1)


class TestPeopleSourceGating(unittest.TestCase):
    def test_weak_person_source_is_deferred_before_generation(self) -> None:
        people = pd.DataFrame([{
            "contractor_name_normalized": "acme construction",
            "contractor_domain": "acme.com",
            "first_name": "John",
            "last_name": "Doe",
            "title": "",
            "title_confirmed": False,
            "source_url": "https://acme.com/",
            "found_email": "",
            "domain_confidence": "high",
        }])

        eligible, deferred, counts = _filter_people_for_personal_generation(people)
        self.assertEqual(len(eligible), 0)
        self.assertEqual(len(deferred), 1)
        self.assertEqual(counts["deferred_low_person_source"], 1)

    def test_team_page_without_title_or_found_email_is_low(self) -> None:
        row = pd.Series({
            "first_name": "John",
            "last_name": "Doe",
            "title_confirmed": False,
            "source_url": "https://acme.com/team",
            "contractor_domain": "acme.com",
            "found_email": "",
        })
        self.assertEqual(_person_source_confidence(row), "low")

    def test_strong_person_source_proceeds_to_generation(self) -> None:
        people = pd.DataFrame([{
            "contractor_name_normalized": "acme construction",
            "contractor_domain": "acme.com",
            "first_name": "John",
            "last_name": "Doe",
            "title": "Project Manager",
            "title_confirmed": True,
            "source_url": "https://acme.com/team",
            "found_email": "",
            "domain_confidence": "high",
        }])

        eligible, deferred, counts = _filter_people_for_personal_generation(people)
        self.assertEqual(len(eligible), 1)
        self.assertEqual(len(deferred), 0)
        self.assertEqual(counts["eligible_people_rows"], 1)

    def test_found_email_person_source_proceeds_to_generation(self) -> None:
        people = pd.DataFrame([{
            "contractor_name_normalized": "acme construction",
            "contractor_domain": "acme.com",
            "first_name": "John",
            "last_name": "Doe",
            "title": "",
            "title_confirmed": False,
            "source_url": "https://acme.com/contact",
            "found_email": "john.doe@acme.com",
            "domain_confidence": "high",
        }])

        eligible, deferred, counts = _filter_people_for_personal_generation(people)
        self.assertEqual(len(eligible), 1)
        self.assertEqual(len(deferred), 0)
        self.assertEqual(counts["eligible_people_rows"], 1)

class TestCompanyNamePropagation(unittest.TestCase):
    def test_apply_clean_company_names_prefers_cleaned_value(self) -> None:
        domains = pd.DataFrame([{
            "contractor_name_normalized": "barnett signs \\4250 action dr, mesquite, tx 75150 /9726818800",
            "cleaned_company_name": "barnett signs",
            "contractor_domain": "barnettsigns.com",
        }])
        out = _apply_clean_company_names(domains)
        self.assertEqual(out.iloc[0]["contractor_name_normalized"], "barnett signs")

if __name__ == "__main__":
    unittest.main()
