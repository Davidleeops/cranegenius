from __future__ import annotations

import unittest
from unittest.mock import patch

from src.domain_discovery import (
    _build_ci_seed_domain_map,
    _build_search_queries,
    _derive_domain_confidence,
    _detect_parked_domain,
    _extract_candidate_domains_from_search_html,
    _search_candidate_score,
    _fuzzy_existing_map_domain,
    clean_company_name,
    discover_company_domains,
    validate_domain,
)


class TestParkedDomainDetection(unittest.TestCase):
    def test_detects_explicit_parking_phrase(self) -> None:
        result = _detect_parked_domain(
            page_text="This domain is parked and available.",
            title="Landing page",
            meta_desc="",
        )
        self.assertTrue(result["parked"])
        self.assertEqual(result["parked_evidence"], "this domain is parked")

    def test_detects_multiple_weak_signals(self) -> None:
        result = _detect_parked_domain(
            page_text="Premium name listed on Sedo.",
            title="This domain is for sale",
            meta_desc="",
        )
        self.assertTrue(result["parked"])
        self.assertEqual(result["parked_evidence"], "for sale, sedo")

    def test_does_not_flag_legitimate_minimal_site(self) -> None:
        result = _detect_parked_domain(
            page_text="Search images maps news gmail drive",
            title="Google",
            meta_desc="Search the world's information",
        )
        self.assertFalse(result["parked"])
        self.assertIsNone(result["parked_evidence"])


class TestCompanyNameCleaning(unittest.TestCase):
    def test_clean_exact_dirty_examples(self) -> None:
        examples = {
            "fcl builders 2401 e randol mill rd, #150, arlington, tx 76011 (972) 672-1824": "fcl builders",
            "alliance elect services 3201 military pkwy a-100, mesquite, tx 75149 (214) 630-8700": "alliance elect services",
            "c2 plumbing 7186096090": "c2 plumbing",
            "gilbane building- 6465601508": "gilbane building",
        }
        for dirty, expected in examples.items():
            self.assertEqual(clean_company_name(dirty), expected)

    def test_does_not_strip_legit_name_starting_with_ste(self) -> None:
        dirty = "steplin construction 7184412500"
        self.assertEqual(clean_company_name(dirty), "steplin construction")

    def test_strips_po_box_tail(self) -> None:
        dirty = "keiser electric po box 151612, ft worth, tx 76108 (817) 319-2796"
        self.assertEqual(clean_company_name(dirty), "keiser electric")

    def test_cleans_backslash_and_slash_contact_tails(self) -> None:
        dirty = "barnett signs \\4250 action dr, mesquite, tx 75150 /9726818800"
        self.assertEqual(clean_company_name(dirty), "barnett signs")


class TestMxOnlyFallback(unittest.TestCase):
    @patch("src.domain_discovery._has_mx_records", return_value=(True, None))
    @patch("src.domain_discovery.requests.head", side_effect=Exception("network"))
    def test_accepts_request_error_when_mx_exists(self, _head, _mx) -> None:
        result = validate_domain("example.com")
        self.assertTrue(result["domain_valid"])
        self.assertTrue(result["mx_valid"])
        self.assertEqual(result["domain_validation_reason"], "valid")

    @patch("src.domain_discovery._has_mx_records", return_value=(True, None))
    @patch("src.domain_discovery.requests.head")
    def test_accepts_403_when_mx_exists(self, mock_head, _mx) -> None:
        class Resp:
            status_code = 403
            url = "https://example.com"
            headers = {}

        mock_head.return_value = Resp()
        result = validate_domain("example.com")
        self.assertTrue(result["domain_valid"])
        self.assertTrue(result["mx_valid"])
        self.assertEqual(result["domain_validation_reason"], "valid")

    @patch("src.domain_discovery._has_mx_records", return_value=(True, None))
    @patch("src.domain_discovery.requests.head")
    def test_keeps_404_rejected_even_if_mx_exists(self, mock_head, _mx) -> None:
        class Resp:
            status_code = 404
            url = "https://example.com"
            headers = {}

        mock_head.return_value = Resp()
        result = validate_domain("example.com")
        self.assertFalse(result["domain_valid"])
        self.assertEqual(result["domain_validation_reason"], "http_404")


class TestAmbiguousMxOnlyGuardrail(unittest.TestCase):
    @patch("src.domain_discovery._has_mx_records", return_value=(True, None))
    @patch("src.domain_discovery.requests.head")
    def test_rejects_aa_domain_for_ambiguous_company(self, mock_head, _mx) -> None:
        class Resp:
            status_code = 403
            url = "https://aa.com"
            headers = {}

        mock_head.return_value = Resp()
        result = validate_domain("aa.com", company_context="a a construction")
        self.assertFalse(result["domain_valid"])
        self.assertEqual(result["domain_validation_reason"], "ambiguous_generic_domain")

    @patch("src.domain_discovery._has_mx_records", return_value=(True, None))
    @patch("src.domain_discovery.requests.head")
    def test_rejects_priority_domain_for_ambiguous_company(self, mock_head, _mx) -> None:
        class Resp:
            status_code = 403
            url = "https://priority.com"
            headers = {}

        mock_head.return_value = Resp()
        result = validate_domain("priority.com", company_context="priority contracting")
        self.assertFalse(result["domain_valid"])
        self.assertEqual(result["domain_validation_reason"], "ambiguous_generic_domain")

    @patch("src.domain_discovery._has_mx_records", return_value=(True, None))
    @patch("src.domain_discovery.requests.head")
    def test_rejects_expert_and_tempo_ambiguous_roots(self, mock_head, _mx) -> None:
        class RespExpert:
            status_code = 403
            url = "https://expert.com"
            headers = {}

        class RespTempo:
            status_code = 403
            url = "https://tempo.com"
            headers = {}

        mock_head.return_value = RespExpert()
        r1 = validate_domain("expert.com", company_context="expert services")
        self.assertFalse(r1["domain_valid"])
        self.assertEqual(r1["domain_validation_reason"], "ambiguous_generic_domain")

        mock_head.return_value = RespTempo()
        r2 = validate_domain("tempo.com", company_context="tempo")
        self.assertFalse(r2["domain_valid"])
        self.assertEqual(r2["domain_validation_reason"], "ambiguous_generic_domain")



    @patch("src.domain_discovery._has_mx_records", return_value=(True, None))
    @patch("src.domain_discovery._check_parking_and_keywords", return_value={"parking": False, "construction_keyword_match": False})
    @patch("src.domain_discovery.requests.head")
    def test_rejects_ambiguous_on_status_200_without_keyword_match(self, mock_head, _content, _mx) -> None:
        class Resp:
            status_code = 200
            url = "https://priority.com"
            headers = {}

        mock_head.return_value = Resp()
        result = validate_domain("priority.com", company_context="priority contracting")
        self.assertFalse(result["domain_valid"])
        self.assertEqual(result["domain_validation_reason"], "ambiguous_generic_domain")

class TestCiSeedIntelligence(unittest.TestCase):
    def test_build_ci_seed_domain_map_uses_numeric_confidence_and_skips_free_domains(self) -> None:
        df = __import__("pandas").DataFrame(
            [
                {
                    "normalized_company_name": "Turner Company",
                    "preferred_domain": "turnerconstruction.com",
                    "domain_confidence": 0.85,
                    "source_support_count": 5,
                },
                {
                    "normalized_company_name": "Turner Company",
                    "preferred_domain": "gmail.com",
                    "domain_confidence": 0.99,
                    "source_support_count": 20,
                },
            ]
        )

        found = _build_ci_seed_domain_map(df)
        self.assertEqual(found.get("turnercompany"), "turnerconstruction.com")

    def test_build_ci_seed_domain_map_skips_conflicting_company_domains(self) -> None:
        df = __import__("pandas").DataFrame(
            [
                {
                    "normalized_company_name": "acme builders",
                    "preferred_domain": "acmebuild.com",
                    "domain_confidence": "high",
                    "source_support_count": 4,
                },
                {
                    "normalized_company_name": "acme builders",
                    "preferred_domain": "acme-group.com",
                    "domain_confidence": "high",
                    "source_support_count": 4,
                },
            ]
        )

        found = _build_ci_seed_domain_map(df)
        self.assertNotIn("acmebuilders", found)

    @patch("src.domain_discovery._load_name_domain_map", return_value={})
    @patch("src.domain_discovery.discover_domain")
    @patch("src.domain_discovery.validate_domain")
    @patch("src.domain_discovery._load_ci_seed_domain_map", return_value={"turnercompany": "turnerconstruction.com"})
    def test_ci_seed_exact_match_precedes_discovery(self, _ci_map, mock_validate, mock_discover, _existing_map) -> None:
        mock_validate.return_value = {
            "domain": "turnerconstruction.com",
            "domain_valid": True,
            "mx_valid": True,
            "domain_validation_reason": "valid",
            "valid": True,
            "construction_keyword_match": True,
        }

        df = __import__("pandas").DataFrame(
            [{"contractor_name_normalized": "Turner Company", "contractor_domain": "", "project_city": "", "project_state": "TX"}]
        )
        out = discover_company_domains(df)

        self.assertEqual(out.iloc[0]["contractor_domain"], "turnerconstruction.com")
        self.assertEqual(out.iloc[0]["domain_discovery_source"], "ci_seed_exact_validated")
        mock_discover.assert_not_called()

    @patch("src.domain_discovery._load_name_domain_map", return_value={})
    @patch("src.domain_discovery._load_ci_seed_domain_map", return_value={})
    @patch("src.domain_discovery.discover_domain")
    def test_weak_or_conflicting_ci_seed_does_not_override_discovery(self, mock_discover, _ci_map, _existing_map) -> None:
        mock_discover.return_value = {
            "domain": "acmebuild.com",
            "domain_valid": True,
            "mx_valid": True,
            "domain_validation_reason": "valid",
            "valid": True,
            "construction_keyword_match": True,
        }

        df = __import__("pandas").DataFrame(
            [{"contractor_name_normalized": "Acme Builders", "contractor_domain": "", "project_city": "", "project_state": "TX"}]
        )
        out = discover_company_domains(df)

        self.assertEqual(out.iloc[0]["contractor_domain"], "acmebuild.com")
        self.assertEqual(out.iloc[0]["domain_discovery_source"], "variant_discovery")



class TestFuzzyExistingMapLookup(unittest.TestCase):
    def test_fuzzy_near_match_returns_domain(self) -> None:
        existing_map = {
            "turnercompany": "turnerconstruction.com",
            "fclbuilders": "fclbuilders.com",
        }
        found = _fuzzy_existing_map_domain("turner compa", existing_map)
        self.assertEqual(found, "turnerconstruction.com")


class TestDomainConfidence(unittest.TestCase):
    def test_domain_confidence_low_for_weak_variant_like_aagroup(self) -> None:
        conf = _derive_domain_confidence(
            is_domain_valid=True,
            source="variant_discovery",
            cleaned_company_name="a a construction",
            domain="aagroup.com",
            result={"domain_validation_reason": "valid", "construction_keyword_match": False},
        )
        self.assertEqual(conf, "low")

    def test_domain_confidence_high_for_existing_input(self) -> None:
        conf = _derive_domain_confidence(
            is_domain_valid=True,
            source="existing_input_validated",
            cleaned_company_name="quick roofing",
            domain="quickroofing.com",
            result={"domain_validation_reason": "valid", "construction_keyword_match": True},
        )
        self.assertEqual(conf, "high")


class TestSearchFallbackHelpers(unittest.TestCase):
    def test_extracts_real_domain_from_mocked_search_html(self) -> None:
        html = """
        <html><body>
          <a href="https://duckduckgo.com/l/?uddg=https%3A%2F%2Fwww.fclbuilders.com%2Fabout">FCL</a>
          <a href="https://duckduckgo.com/l/?uddg=https%3A%2F%2Fwww.linkedin.com%2Fcompany%2Ffcl">LinkedIn</a>
        </body></html>
        """
        domains = _extract_candidate_domains_from_search_html(html)
        self.assertIn("fclbuilders.com", domains)

    def test_excludes_directory_and_social_domains(self) -> None:
        html = """
        <html><body>
          <a href="https://duckduckgo.com/l/?uddg=https%3A%2F%2Fwww.linkedin.com%2Fcompany%2Facme">LinkedIn</a>
          <a href="https://duckduckgo.com/l/?uddg=https%3A%2F%2Fwww.manta.com%2Fc%2Fabc">Manta</a>
          <a href="https://duckduckgo.com/l/?uddg=https%3A%2F%2Facmeconstruction.com">Acme</a>
        </body></html>
        """
        domains = _extract_candidate_domains_from_search_html(html)
        self.assertEqual(domains, ["acmeconstruction.com"])


class TestSearchFallbackScoring(unittest.TestCase):
    def test_search_score_prefers_company_token_match(self) -> None:
        a = _search_candidate_score("fcl builders", "fclbuilders.com")
        b = _search_candidate_score("fcl builders", "bestcontractorhub.com")
        self.assertGreater(a, b)



class TestSearchFallbackInvocation(unittest.TestCase):
    @patch("src.domain_discovery._resolve_domain_via_search")
    @patch("src.domain_discovery._load_name_domain_map", return_value={})
    @patch("src.domain_discovery.discover_domain")
    def test_search_fallback_only_after_prior_failure(self, mock_discover, _map, mock_search) -> None:
        mock_discover.side_effect = [
            {"domain": "fclbuilders.com", "domain_valid": True, "mx_valid": True, "domain_validation_reason": "valid", "valid": True},
            {"domain": None, "domain_valid": False, "mx_valid": False, "domain_validation_reason": "no_valid_domain", "valid": False},
        ]
        mock_search.return_value = {"result": None, "search_query": "", "search_candidate_domain": ""}

        df = __import__("pandas").DataFrame([
            {"contractor_name_normalized": "fcl builders", "contractor_domain": "", "project_city": "", "project_state": "TX"},
            {"contractor_name_normalized": "unknown builder", "contractor_domain": "", "project_city": "Dallas", "project_state": "TX"},
        ])
        _ = discover_company_domains(df)

        # called only for unresolved row
        self.assertEqual(mock_search.call_count, 1)



if __name__ == "__main__":
    unittest.main()
