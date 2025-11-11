import unittest
from unittest.mock import patch, Mock
import pandas as pd
from scrapers.sweed_scraper import _get_all_variant_info, _get_unique_details, fetch_sweed_data

class TestSweedScraper(unittest.TestCase):

    def setUp(self):
        """Set up mock data for Sweed scraper tests."""
        self.mock_product_list_json = {
            "list": [
                {
                    "name": "Test Flower",
                    "brand": {"name": "Test Brand"},
                    "subcategory": {"name": "Flower"},
                    "variants": [
                        {"id": 12345, "name": "3.5g"}
                    ]
                },
                {
                    "name": "Test Vape",
                    "brand": {"name": "Another Brand"},
                    "subcategory": {"name": "CARTRIDGES"},
                    "variants": [
                        {"id": 67890, "name": "0.5g"}
                    ]
                }
            ]
        }
        self.mock_variant_detail_json = {
            "variants": [{
                "price": 50.0,
                "promoPrice": 45.0,
                "name": "3.5g",
                "unitSize": {"value": 3.5, "unitAbbr": "G"}
            }]
        }
        self.mock_lab_data_json = {
            "thc": {"values": [{"code": "THCA", "min": 22.5}]},
            "cbd": {"values": [{"code": "CBD", "min": 0.5}]},
            "terpenes": {"values": [
                {"code": "Limonene", "min": 0.8},
                {"code": "b_caryophyllene", "min": 0.6}
            ]}
        }

    @patch('scrapers.sweed_scraper.requests.post')
    @patch('scrapers.sweed_scraper.SWED_STORES_TO_SCRAPE', {"Test Store": 1})
    @patch('scrapers.sweed_scraper.CATEGORY_MAP', {"Test Category": 1})
    def test_get_all_variant_info(self, mock_post):
        """Test the function that gathers basic variant info from all stores."""
        # Mock the API response
        mock_response = Mock()
        mock_response.json.return_value = self.mock_product_list_json
        # Mock a second, empty response to terminate the pagination loop
        mock_empty_response = Mock()
        mock_empty_response.json.return_value = {"list": []}
        mock_post.side_effect = [mock_response, mock_empty_response]

        result = _get_all_variant_info()

        # We expect 2 variants from the mock data
        self.assertEqual(len(result), 2)

        # Check the first variant
        self.assertEqual(result[0]['variant_id'], 12345)
        self.assertEqual(result[0]['Name'], "Test Flower")
        self.assertEqual(result[0]['Brand'], "Test Brand")
        self.assertEqual(result[0]['Store'], "Test Store")
        self.assertEqual(result[0]['Type'], "Test Category")
        self.assertEqual(result[0]['Subtype'], "Flower")

        # Check the second variant
        self.assertEqual(result[1]['variant_id'], 67890)
        self.assertEqual(result[1]['Brand'], "Another Brand")
        # Example of checking a mapped subcategory
        self.assertEqual(result[1]['Subtype'], "Cartridge")

    @patch('scrapers.sweed_scraper.requests.post')
    def test_get_unique_details(self, mock_post):
        """Test the function that fetches detailed data for unique variants."""
        # Mock the two API responses needed for a single variant
        mock_variant_resp = Mock()
        mock_variant_resp.json.return_value = self.mock_variant_detail_json
        mock_lab_resp = Mock()
        mock_lab_resp.json.return_value = self.mock_lab_data_json

        mock_post.side_effect = [mock_variant_resp, mock_lab_resp]

        unique_ids = {12345}
        result = _get_unique_details(unique_ids)

        # Check that we got details for our one ID
        self.assertIn(12345, result)
        self.assertEqual(len(result), 1)

        details = result[12345]

        # Check parsed details
        self.assertEqual(details['Price'], 45.0) # Should use promoPrice
        self.assertEqual(details['Weight'], 3.5)
        self.assertEqual(details['THCa'], 22.5)
        self.assertEqual(details['CBD'], 0.5)
        self.assertEqual(details['Limonene'], 0.8)
        self.assertEqual(details['beta-Caryophyllene'], 0.6)

    @patch('scrapers.sweed_scraper._get_all_variant_info')
    @patch('scrapers.sweed_scraper._get_unique_details')
    def test_fetch_sweed_data_end_to_end(self, mock_get_details, mock_get_variants):
        """Test the main fetch_sweed_data function end-to-end."""
        # Mock the return values of the helper functions
        mock_get_variants.return_value = [
            {'variant_id': 12345, 'Name': 'Test Flower', 'Brand': 'Test Brand', 'Store': 'Test Store'},
            {'variant_id': 67890, 'Name': 'Test Vape', 'Brand': 'Another Brand', 'Store': 'Test Store'}
        ]
        mock_get_details.return_value = {
            12345: {'Price': 45.0, 'Weight': 3.5, 'THCa': 22.5},
            67890: {'Price': 30.0, 'Weight': 0.5, 'THCa': 85.0}
        }

        df = fetch_sweed_data()

        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 2)
        self.assertIn('dpg', df.columns)

        # Check data for the first product
        self.assertEqual(df.iloc[0]['Name'], 'Test Flower')
        self.assertEqual(df.iloc[0]['Price'], 45.0)
        self.assertAlmostEqual(df.iloc[0]['dpg'], 45.0 / 3.5)

        # Check data for the second product
        self.assertEqual(df.iloc[1]['Brand'], 'Another Brand')
        self.assertEqual(df.iloc[1]['Weight'], 0.5)
        self.assertEqual(df.iloc[1]['THCa'], 85.0)


if __name__ == '__main__':
    unittest.main()
