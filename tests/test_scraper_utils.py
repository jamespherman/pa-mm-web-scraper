# tests/test_scraper_utils.py

import sys
import os
import unittest

# Add the parent directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scrapers.scraper_utils import convert_to_grams

class TestConvertToGrams(unittest.TestCase):

    def test_valid_weights(self):
        self.assertAlmostEqual(convert_to_grams('gram'), 1.0)
        self.assertAlmostEqual(convert_to_grams('1g'), 1.0)
        self.assertAlmostEqual(convert_to_grams('half gram'), 0.5)
        self.assertAlmostEqual(convert_to_grams('0.5g'), 0.5)
        self.assertAlmostEqual(convert_to_grams('eighth ounce'), 3.5)
        self.assertAlmostEqual(convert_to_grams('3.5g'), 3.5)
        self.assertAlmostEqual(convert_to_grams('quarter ounce'), 7.0)
        self.assertAlmostEqual(convert_to_grams('7g'), 7.0)
        self.assertAlmostEqual(convert_to_grams('half ounce'), 14.0)
        self.assertAlmostEqual(convert_to_grams('14g'), 14.0)
        self.assertAlmostEqual(convert_to_grams('ounce'), 28.0)
        self.assertAlmostEqual(convert_to_grams('28g'), 28.0)
        self.assertAlmostEqual(convert_to_grams('1/8oz'), 3.5)
        self.assertAlmostEqual(convert_to_grams('1/4oz'), 7.0)
        self.assertAlmostEqual(convert_to_grams('1/2oz'), 14.0)
        self.assertAlmostEqual(convert_to_grams('1oz'), 28.0)

    def test_invalid_weights(self):
        self.assertIsNone(convert_to_grams('invalid string'))
        self.assertIsNone(convert_to_grams('1.0'))
        self.assertIsNone(convert_to_grams(None))
        self.assertIsNone(convert_to_grams(123))

    def test_case_insensitivity(self):
        self.assertAlmostEqual(convert_to_grams('Gram'), 1.0)
        self.assertAlmostEqual(convert_to_grams('EIGHTH OUNCE'), 3.5)

    def test_whitespace(self):
        self.assertAlmostEqual(convert_to_grams(' 1g '), 1.0)
        self.assertAlmostEqual(convert_to_grams(' 3.5g'), 3.5)

if __name__ == '__main__':
    unittest.main()
