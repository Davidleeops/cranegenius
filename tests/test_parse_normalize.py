from __future__ import annotations

import unittest

from src.parse_normalize import clean_contractor_name


class TestParseNormalizeCompanyCleaning(unittest.TestCase):
    def test_cleans_address_phone_and_slash_fragments(self) -> None:
        dirty = "barnett signs \\4250 action dr, mesquite, tx 75150 /9726818800"
        self.assertEqual(clean_contractor_name(dirty), "barnett signs")


if __name__ == "__main__":
    unittest.main()
