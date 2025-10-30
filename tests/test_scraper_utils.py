import unittest
from scrapers.scraper_utils import convert_to_grams

class TestConvertToGrams(unittest.TestCase):
    def test_valid_conversions(self):
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

    def test_new_conversions(self):
        self.assertAlmostEqual(convert_to_grams('two gram'), 2.0)
        self.assertAlmostEqual(convert_to_grams('1g'), 1.0)
        self.assertAlmostEqual(convert_to_grams('0.5g'), 0.5)
        self.assertAlmostEqual(convert_to_grams('3.5g'), 3.5)
        self.assertAlmostEqual(convert_to_grams('7g'), 7.0)
        self.assertAlmostEqual(convert_to_grams('14g'), 14.0)
        self.assertAlmostEqual(convert_to_grams('28g'), 28.0)

    def test_invalid_conversions(self):
        self.assertIsNone(convert_to_grams('invalid string'))
        self.assertIsNone(convert_to_grams(None))
        self.assertIsNone(convert_to_grams(123))

    def test_mg_conversion(self):
        self.assertAlmostEqual(convert_to_grams('1 mg'), 0.001)

if __name__ == '__main__':
    unittest.main()
