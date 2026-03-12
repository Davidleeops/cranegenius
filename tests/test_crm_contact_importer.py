from __future__ import annotations

import unittest

from src.crm_contact_importer import (
    _clean_company_name,
    _domain_from_email,
    _domain_from_website,
    _is_free_or_isp_domain,
)


class TestCrmContactImporterHelpers(unittest.TestCase):
    def test_domain_from_email_extracts_valid_domain(self) -> None:
        self.assertEqual(_domain_from_email("John.Doe@AcmeConstruction.com"), "acmeconstruction.com")
        self.assertEqual(_domain_from_email("not-an-email"), "")

    def test_domain_from_website_extracts_registered_domain(self) -> None:
        self.assertEqual(_domain_from_website("acmeconstruction.com/contact"), "acmeconstruction.com")
        self.assertEqual(_domain_from_website("https://www.quickroofing.com/about"), "quickroofing.com")

    def test_free_isp_classification(self) -> None:
        self.assertTrue(_is_free_or_isp_domain("gmail.com"))
        self.assertTrue(_is_free_or_isp_domain("mail.twc.rr.com"))
        self.assertFalse(_is_free_or_isp_domain("acmeconstruction.com"))

    def test_clean_company_name_removes_address_noise(self) -> None:
        dirty = "Barnett Signs \\4250 Action Dr, Mesquite, TX 75150 /9726818800"
        self.assertEqual(_clean_company_name(dirty), "barnett signs")


if __name__ == "__main__":
    unittest.main()
