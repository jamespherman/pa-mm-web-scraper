import unittest
from unittest.mock import patch, Mock
import pandas as pd
from scrapers.dutchie_scraper import parse_product_details, fetch_dutchie_data, DUTCHIE_STORES

class TestDutchieScraper(unittest.TestCase):

    def setUp(self):
        """Set up mock product dictionaries for use in tests."""
        self.mock_product_json = {
            "Name": "Test Kush",
            "brandName": "Test Farms",
            "type": "Flower",
            "subcategory": "Indica",
            "medicalPrices": [50.0],
            "medicalSpecialPrices": [40.0],
            "Options": ["3.5g"],
            "cannabinoidsV2": [
                {"cannabinoid": {"name": "THCA"}, "value": 22.1},
                {"cannabinoid": {"name": "THC"}, "value": 0.8},
                {"cannabinoid": {"name": "CBD"}, "value": 0.1}
            ],
            "terpenes": [
                {"libraryTerpene": {"name": "b_myrcene"}, "value": 0.85},
                {"libraryTerpene": {"name": "Limonene"}, "value": 0.45},
                {"libraryTerpene": {"name": "unknown-terp"}, "value": 0.1}
            ]
        }

        self.mock_product_missing_data = {
            "Name": "Ghost OG",
            "brandName": "Mystery Brand",
            "type": "Concentrates",
            "subcategory": None,
            "medicalPrices": [60.0],
            "medicalSpecialPrices": [],
            "Options": ["1g"],
            "cannabinoidsV2": [
                {"cannabinoid": {"name": "THC"}, "value": 85.5}
            ],
            "terpenes": []
        }

    def test_parse_product_details_happy_path(self):
        """Test parsing a standard, well-formed product JSON."""
        parsed_data = parse_product_details(self.mock_product_json, "Test Store")

        self.assertEqual(parsed_data['Name'], "Test Kush")
        self.assertEqual(parsed_data['Brand'], "Test Farms")
        self.assertEqual(parsed_data['Store'], "Test Store")
        self.assertEqual(parsed_data['Type'], "Flower")
        self.assertEqual(parsed_data['Subtype'], "Indica")
        self.assertEqual(parsed_data['Price'], 40.0)
        self.assertEqual(parsed_data['Weight'], 3.5)
        self.assertEqual(parsed_data['Weight_Str'], "3.5g")
        self.assertEqual(parsed_data['THCa'], 22.1)
        self.assertEqual(parsed_data['THC'], 0.8)
        self.assertEqual(parsed_data['CBD'], 0.1)
        self.assertEqual(parsed_data['beta-Myrcene'], 0.85)
        self.assertEqual(parsed_data['Limonene'], 0.45)
        self.assertNotIn('unknown-terp', parsed_data)
        self.assertNotIn('Linalool', parsed_data)

    def test_parse_product_with_missing_data(self):
        """Test parsing a product with missing terpenes and cannabinoids."""
        parsed_data = parse_product_details(self.mock_product_missing_data, "Test Store")

        self.assertEqual(parsed_data['Name'], "Ghost OG")
        self.assertIsNone(parsed_data['Subtype'])
        self.assertEqual(parsed_data['Price'], 60.0)
        self.assertEqual(parsed_data['THC'], 85.5)
        self.assertNotIn('CBD', parsed_data)
        self.assertNotIn('beta-Myrcene', parsed_data)

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
