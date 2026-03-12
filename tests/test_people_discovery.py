from __future__ import annotations

import unittest

from src.contact_page_finder import _extract_candidates
from src.people_discovery import _extract_from_page, _normalize_person_tokens


class TestNameQualityFilters(unittest.TestCase):
    def test_about_locations_rejected(self) -> None:
        first, last, _ = _normalize_person_tokens(["About", "Locations"])
        self.assertIsNone(first)
        self.assertIsNone(last)

    def test_contact_resources_rejected(self) -> None:
        first, last, _ = _normalize_person_tokens(["Contact", "Resources"])
        self.assertIsNone(first)
        self.assertIsNone(last)

    def test_years_experience_rejected(self) -> None:
        first, last, _ = _normalize_person_tokens(["Years", "Experience"])
        self.assertIsNone(first)
        self.assertIsNone(last)

    def test_featured_an_rejected(self) -> None:
        first, last, _ = _normalize_person_tokens(["Featured", "An"])
        self.assertIsNone(first)
        self.assertIsNone(last)

    def test_article_living_rejected(self) -> None:
        first, last, _ = _normalize_person_tokens(["Article", "Living"])
        self.assertIsNone(first)
        self.assertIsNone(last)

    def test_magazine_adrian_rejected(self) -> None:
        first, last, _ = _normalize_person_tokens(["Magazine", "Adrian"])
        self.assertIsNone(first)
        self.assertIsNone(last)

    def test_comments_the_rejected(self) -> None:
        first, last, _ = _normalize_person_tokens(["Comments", "The"])
        self.assertIsNone(first)
        self.assertIsNone(last)

    def test_realistic_person_still_passes(self) -> None:
        first, last, _ = _normalize_person_tokens(["John", "Martinez"])
        self.assertEqual(first, "John")
        self.assertEqual(last, "Martinez")

    def test_cta_phrase_new_roof_rejected(self) -> None:
        first, last, _ = _normalize_person_tokens(["New", "Roof"])
        self.assertIsNone(first)
        self.assertIsNone(last)

    def test_cta_phrase_request_quote_rejected(self) -> None:
        first, last, _ = _normalize_person_tokens(["Request", "Quote"])
        self.assertIsNone(first)
        self.assertIsNone(last)

    def test_unconfirmed_cta_like_context_not_extracted_as_person(self) -> None:
        html = "<html><body><div>Learn More</div></body></html>"
        rows = _extract_from_page(html, "https://acme.com", "acme", "acme.com", "", "")
        self.assertEqual(rows, [])

    def test_company_echo_quick_roofing_rejected(self) -> None:
        html = "<html><body><div>Quick Roofing</div></body></html>"
        rows = _extract_from_page(html, "https://quickroofing.com", "quick roofing", "quickroofing.com", "", "")
        self.assertEqual(rows, [])

    def test_content_page_context_blocks_name_extraction(self) -> None:
        html = "<html><body><div>John Martinez</div></body></html>"
        rows = _extract_from_page(html, "https://acme.com/blog/post-1", "acme", "acme.com", "", "")
        self.assertEqual(rows, [])

    def test_contact_page_blocks_nav_like_name_near_email(self) -> None:
        html = "<html><body>Contact Resources info@acme.com</body></html>"
        rows = _extract_candidates(html, "acme.com", "https://acme.com/contact")
        self.assertTrue(rows)
        self.assertEqual(rows[0]["first_name"], "")
        self.assertEqual(rows[0]["last_name"], "")


if __name__ == "__main__":
    unittest.main()
