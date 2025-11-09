import unittest
from unittest.mock import patch, Mock
import pandas as pd
import numpy as np
from scrapers.dutchie_scraper import parse_product_details, fetch_dutchie_data, DUTCHIE_STORES

class TestDutchieScraper(unittest.TestCase):

    def setUp(self):
        """Set up a mock product dictionary for use in tests."""
        self.mock_product_json = {
            "Name": "Test Kush",
            "brandName": "Test Farms",
            "type": "Flower",
            "subcategory": "Indica",
            "medicalPrices": [50.0],
            "medicalSpecialPrices": [40.0], # Has a special price
            "Options": ["3.5g"],
            "cannabinoidsV2": [
                {"name": "THCa", "value": 22.1},
                {"name": "THC", "value": 0.8},
                {"name": "CBD", "value": 0.1}
            ],
            "terpenes": [
                {"name": "b-myrcene", "value": 0.85}, # Test lowercase and mapping
                {"name": "Limonene", "value": 0.45},
                {"name": "unknown-terp", "value": 0.1} # This one should be ignored
            ],
            # To test fallbacks
            "THCContent": {"range": [0.8, 1.2]},
            "CBDContent": {"range": [0.1, 0.2]}
        }

        self.mock_product_missing_data = {
            "Name": "Ghost OG",
            "brandName": "Mystery Brand",
            "type": "Concentrate",
            "subcategory": None,
            "medicalPrices": [60.0],
            "medicalSpecialPrices": [],
            "Options": ["1g"],
            "cannabinoidsV2": [], # Empty cannabinoids
            "terpenes": [], # Empty terpenes
            "THCContent": {"range": [85.5, 86.0]}, # Fallback THC
            "CBDContent": {"range": []} # No CBD
        }

    def test_parse_product_details_happy_path(self):
        """Test parsing a standard, well-formed product JSON."""
        parsed_data = parse_product_details(self.mock_product_json, "Test Store")

        self.assertEqual(parsed_data['Name'], "Test Kush")
        self.assertEqual(parsed_data['Brand'], "Test Farms")
        self.assertEqual(parsed_data['Store'], "Test Store")
        self.assertEqual(parsed_data['Type'], "flower")
        self.assertEqual(parsed_data['Subtype'], "Indica")

        # Test special price is chosen
        self.assertEqual(parsed_data['Price'], 40.0)

        # Test weight conversion
        self.assertEqual(parsed_data['Weight'], 3.5)
        self.assertEqual(parsed_data['Weight_Str'], "3.5g")

        # Test cannabinoids
        self.assertEqual(parsed_data['THCa'], 22.1)
        self.assertEqual(parsed_data['THC'], 0.8)
        self.assertEqual(parsed_data['CBD'], 0.1)

        # Test terpenes and mapping
        self.assertEqual(parsed_data['beta-Myrcene'], 0.85)
        self.assertEqual(parsed_data['Limonene'], 0.45)
        self.assertAlmostEqual(parsed_data['Total_Terps'], 1.30)

        # Check that unknown terp is not in the final dict
        self.assertNotIn('unknown-terp', parsed_data)
        # Check that a known terp not in the product is present but NaN
        self.assertTrue(np.isnan(parsed_data['Linalool']))

    def test_parse_product_with_missing_data(self):
        """Test parsing a product with missing terpenes and cannabinoids."""
        parsed_data = parse_product_details(self.mock_product_missing_data, "Test Store")

        self.assertEqual(parsed_data['Name'], "Ghost OG")
        self.assertIsNone(parsed_data['Subtype'])

        # Test price fallback
        self.assertEqual(parsed_data['Price'], 60.0)

        # Test cannabinoid fallback
        self.assertEqual(parsed_data['THC'], 85.5)
        self.assertNotIn('CBD', parsed_data) # Should not be present if range is empty

        # Test terpenes
        self.assertTrue(np.isnan(parsed_data['Total_Terps']))
        self.assertTrue(np.isnan(parsed_data['beta-Myrcene'])) # Should be NaN

    @patch('scrapers.dutchie_scraper.requests.get')
    def test_fetch_dutchie_data_flow(self, mock_get):
        """Test the end-to-end flow of fetching slugs and then details."""

        # --- Mock Setup ---

        # Mock response for get_all_product_slugs
        mock_slugs_response = Mock()
        mock_slugs_response.json.return_value = {
            "data": {
                "filteredProducts": {
                    "products": [
                        {"cName": "test-kush-123"},
                        {"cName": "ghost-og-456"}
                    ]
                }
            }
        }
        # A second response for the second page/category call which is empty
        mock_empty_response = Mock()
        mock_empty_response.json.return_value = {"data": {"filteredProducts": {"products": []}}}

        # Mock response for get_detailed_product_info
        mock_detail_response_1 = Mock()
        mock_detail_response_1.json.return_value = {
            "data": {
                "filteredProducts": {
                    "products": [self.mock_product_json]
                }
            }
        }
        mock_detail_response_2 = Mock()
        mock_detail_response_2.json.return_value = {
            "data": {
                "filteredProducts": {
                    "products": [self.mock_product_missing_data]
                }
            }
        }

        # Set the side_effect to return responses in order
        num_stores = len(DUTCHIE_STORES)

        # Slugs are fetched once per store, with a second call for an empty page to stop pagination.
        slug_calls = [mock_slugs_response, mock_empty_response] * num_stores

        # Details are fetched for each slug (2 slugs per store).
        detail_calls = [mock_detail_response_1, mock_detail_response_2] * num_stores

        mock_get.side_effect = slug_calls + detail_calls

        # --- Execute ---
        df = fetch_dutchie_data()

        # --- Assertions ---
        self.assertIsInstance(df, pd.DataFrame)
        # Expected: 2 products per store * number of stores
        self.assertEqual(len(df), 2 * num_stores)

        # Check data from the first product
        self.assertEqual(df.iloc[0]['Name'], 'Test Kush')
        self.assertEqual(df.iloc[0]['Price'], 40.0)
        self.assertEqual(df.iloc[0]['THCa'], 22.1)

        # Check data from the second product
        self.assertEqual(df.iloc[1]['Name'], 'Ghost OG')
        self.assertEqual(df.iloc[1]['Price'], 60.0)
        self.assertEqual(df.iloc[1]['THC'], 85.5)

if __name__ == '__main__':
    unittest.main()
