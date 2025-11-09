import unittest
from unittest.mock import patch, Mock
import pandas as pd
import numpy as np
from scrapers.cresco_scraper import parse_cresco_products, fetch_cresco_data, extract_weight_from_cresco_name

class TestCrescoScraper(unittest.TestCase):

    def setUp(self):
        """Set up mock data for Cresco scraper tests."""
        self.mock_products_json = [
            {
                "name": "Cresco Labs: GG#4 3.5g",
                "brand": "Cresco",
                "category": "flower",
                "price": 55.0,
                "discounted_price": 45.0,
                "bt_potency_thc": 0.9,
                "bt_potency_thca": 23.5,
                "bt_potency_cbd": 0.1,
                "bt_potency_terps": 2.1,
                "terpenes": [
                    {"terpene": "beta_caryophyllene", "value": 0.8},
                    {"terpene": "limonene", "value": 0.5}
                ]
            },
            {
                "name": "Remedi: Indica Tincture 500mg",
                "brand": "Remedi",
                "category": "tinctures",
                "price": 60.0,
                "discounted_price": None,
                "bt_potency_thc": 15.0,
                "bt_potency_thca": None,
                "bt_potency_cbd": 15.0,
                "terpenes": [] # No terpenes listed
            }
        ]

    def test_extract_weight_from_name(self):
        """Test the regex function for extracting weight from product names."""
        self.assertEqual(extract_weight_from_cresco_name("Product Name 3.5g"), 3.5)
        self.assertEqual(extract_weight_from_cresco_name("Product 1g"), 1.0)
        self.assertEqual(extract_weight_from_cresco_name("Product 500mg"), 0.5)
        self.assertEqual(extract_weight_from_cresco_name("Product .5g"), 0.5)
        self.assertTrue(np.isnan(extract_weight_from_cresco_name("No weight here")))

    def test_parse_cresco_products(self):
        """Test the parsing of a list of products from Cresco JSON."""
        parsed_data = parse_cresco_products(self.mock_products_json, "Test Store")

        self.assertEqual(len(parsed_data), 2)

        # --- Test the flower product ---
        product1 = parsed_data[0]
        self.assertEqual(product1['Name'], "Cresco Labs: GG#4 3.5g")
        self.assertEqual(product1['Brand'], "Cresco")
        self.assertEqual(product1['Store'], "Test Store")
        self.assertEqual(product1['Price'], 45.0) # Discounted price
        self.assertEqual(product1['Weight'], 3.5)
        self.assertEqual(product1['THCa'], 23.5)
        self.assertEqual(product1['THC'], 0.9)
        self.assertEqual(product1['beta-Caryophyllene'], 0.8)
        self.assertEqual(product1['Limonene'], 0.5)
        # Total terps should be SUM of terpenes list, not the fallback value
        self.assertAlmostEqual(product1['Total_Terps'], 1.3)
        self.assertIsNone(product1['Subtype'])

        # --- Test the tincture product ---
        product2 = parsed_data[1]
        self.assertEqual(product2['Name'], "Remedi: Indica Tincture 500mg")
        self.assertEqual(product2['Price'], 60.0) # Regular price
        self.assertEqual(product2['Weight'], 0.5)
        self.assertIsNone(product2['THCa'])
        self.assertTrue(np.isnan(product2['Total_Terps'])) # No terpenes

    @patch('scrapers.cresco_scraper.requests.get')
    def test_fetch_cresco_data_flow(self, mock_get):
        """Test the end-to-end data fetching and parsing flow for Cresco."""

        # Define a mock store for the test
        STORES = {"Test Store": "123"}

        # Mock response for a page with data
        mock_response_with_data = Mock()
        mock_response_with_data.json.return_value = {"data": self.mock_products_json}

        # Mock response for a page with no data (to stop the pagination loop)
        mock_response_no_data = Mock()
        mock_response_no_data.json.return_value = {"data": []}

        # Simulate one category having one page of data, and the rest empty
        mock_get.side_effect = [
            mock_response_with_data, # Page 1 of flower
            mock_response_no_data,   # Page 2 of flower (empty, stops loop)
            mock_response_no_data,   # vapes
            mock_response_no_data    # concentrates
        ]

        df = fetch_cresco_data(STORES)

        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 2)
        self.assertEqual(df.iloc[0]['Name'], 'Cresco Labs: GG#4 3.5g')
        self.assertEqual(df.iloc[1]['Brand'], 'Remedi')

if __name__ == '__main__':
    unittest.main()
