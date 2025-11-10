import unittest
from unittest.mock import patch, Mock
import pandas as pd
from scrapers.trulieve_scraper import parse_trulieve_products, fetch_trulieve_data

class TestTrulieveScraper(unittest.TestCase):

    def setUp(self):
        """Set up mock data for Trulieve scraper tests."""
        self.mock_products_json = [
            {
                "name": "Super Lemon Haze",
                "brand": "Trulieve",
                "category": "flower",
                "subcategory": "Sativa Flower",
                "thc_content": 20.5,
                "cbd_content": 0.5,
                "variants": [
                    {"option": "3.5g", "unit_price": 45.0, "sale_unit_price": None},
                    {"option": "7g", "unit_price": 80.0, "sale_unit_price": 70.0}
                ]
            },
            {
                "name": "9LB Hammer",
                "brand": "Modern Flower",
                "category": "concentrates",
                "subcategory": "Rosin",
                "thc_content": 85.0,
                "cbd_content": None,
                "variants": [
                    {"option": "1g", "unit_price": 60.0, "sale_unit_price": None}
                ]
            }
        ]

    def test_parse_trulieve_products(self):
        """Test the parsing of a list of products from Trulieve JSON."""
        parsed_data = parse_trulieve_products(self.mock_products_json, "Test Store")
        self.assertEqual(len(parsed_data), 3)

        variant1 = parsed_data[0]
        self.assertEqual(variant1['Name'], "Super Lemon Haze")
        self.assertEqual(variant1['Brand'], "Trulieve")
        self.assertEqual(variant1['Store'], "Test Store")
        self.assertEqual(variant1['Weight'], 3.5)
        self.assertEqual(variant1['Price'], 45.0)
        self.assertEqual(variant1['THC'], 20.5)
        self.assertEqual(variant1['CBD'], 0.5)

        variant2 = parsed_data[1]
        self.assertEqual(variant2['Weight'], 7.0)
        self.assertEqual(variant2['Price'], 70.0)

        variant3 = parsed_data[2]
        self.assertEqual(variant3['Name'], "9LB Hammer")
        self.assertEqual(variant3['Weight'], 1.0)
        self.assertEqual(variant3['Price'], 60.0)
        self.assertEqual(variant3['THC'], 85.0)
        self.assertIsNone(variant3['CBD'])

    @patch('scrapers.trulieve_scraper.requests.get')
    def test_fetch_trulieve_data_flow(self, mock_get):
        """Test the end-to-end data fetching and parsing flow for Trulieve."""

        # Mock response for a page with data
        mock_response_with_data = Mock()
        mock_response_with_data.json.return_value = {
            "data": self.mock_products_json,
            "current_page": 1,
            "last_page": 1 # This will stop the pagination loop
        }

        # Mock response for a page with no data (to stop the loop for other categories)
        mock_response_no_data = Mock()
        mock_response_no_data.json.return_value = {"data": []}

        # Simulate one category ("flower") having data and the rest being empty
        mock_get.side_effect = [
            # Store 1
            mock_response_with_data, # Flower
            mock_response_no_data,   # Vaporizers
            mock_response_no_data,   # Concentrates
            mock_response_no_data,   # Tinctures
            mock_response_no_data,   # Edibles
            # Store 2
            mock_response_with_data, # Flower
            mock_response_no_data,   # Vaporizers
            mock_response_no_data,   # Concentrates
            mock_response_no_data,   # Tinctures
            mock_response_no_data    # Edibles
        ]

        STORES = {
            "Trulieve (Squirrel Hill)": "86",
            "Trulieve (North Shore)": "90"
        }
        df = fetch_trulieve_data(STORES)

        self.assertIsInstance(df, pd.DataFrame)
        # We expect 3 rows per store (2 stores total)
        self.assertEqual(len(df), 6)

        # Check that data from both stores is present
        self.assertIn("Trulieve (Squirrel Hill)", df['Store'].unique())
        self.assertIn("Trulieve (North Shore)", df['Store'].unique())

        # A quick check on a value
        self.assertEqual(df.iloc[0]['Name'], 'Super Lemon Haze')

if __name__ == '__main__':
    unittest.main()
