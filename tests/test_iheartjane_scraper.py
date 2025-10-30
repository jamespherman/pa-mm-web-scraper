import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from scrapers.iheartjane_scraper import fetch_iheartjane_data, parse_jane_product

class TestIHeartJaneScraper(unittest.TestCase):

    def setUp(self):
        """Set up mock data for iHeartJane scraper tests."""
        self.mock_product_hit_flower = {
            "search_attributes": {
                "name": "Test Flower",
                "brand": "Test Brand",
                "kind": "Flower",
                "kind_subtype": "Hybrid",
                "available_weights": ["eighth_ounce", "quarter_ounce"],
                "price_eighth_ounce": 35.0,
                "price_quarter_ounce": 60.0,
                "special_price_quarter_ounce": {"discount_price": 55.0},
                "store_notes": "Terpene Profile: beta-Myrcene: 1.2%, Limonene 0.8%",
                "inventory_potencies": [
                    {"price_id": "eighth_ounce", "thca_potency": 22.5, "cbd_potency": 0.5}
                ]
            }
        }
        self.mock_product_hit_vape = {
            "search_attributes": {
                "name": "Test Vape",
                "brand": "Another Brand",
                "kind": "Vape",
                "price_each": 50.0,
                "description": "b-Caryophyllene: 2.5%",
                "inventory_potencies": [
                    {"price_id": "each", "thca_potency": 90.0, "cbg_potency": 2.0}
                ]
            }
        }

    def test_parse_jane_product_variants(self):
        """Test parsing a single product hit with multiple weight variants."""
        variants = parse_jane_product(self.mock_product_hit_flower, "Test Store")

        # Expect two rows, one for each weight
        self.assertEqual(len(variants), 2)

        # Test eighth ounce variant
        variant1 = variants[0]
        self.assertEqual(variant1['Name'], "Test Flower")
        self.assertEqual(variant1['Weight_Str'], "eighth_ounce")
        self.assertEqual(variant1['Weight'], 3.5)
        self.assertEqual(variant1['Price'], 35.0)
        self.assertEqual(variant1['THCa'], 22.5)
        self.assertEqual(variant1['beta-Myrcene'], 1.2)

        # Test quarter ounce variant (with special price)
        variant2 = variants[1]
        self.assertEqual(variant2['Weight_Str'], "quarter_ounce")
        self.assertEqual(variant2['Weight'], 7.0)
        self.assertEqual(variant2['Price'], 55.0) # Check that special price is used
        self.assertEqual(variant2['THCa'], 22.5) # Potency should carry over

    def test_parse_jane_product_each(self):
        """Test parsing a product sold by 'each'."""
        variants = parse_jane_product(self.mock_product_hit_vape, "Test Store")
        self.assertEqual(len(variants), 1)
        variant = variants[0]
        self.assertEqual(variant['Name'], "Test Vape")
        self.assertEqual(variant['Weight_Str'], "Each")
        self.assertIsNone(variant['Weight'])
        self.assertEqual(variant['Price'], 50.0)
        self.assertEqual(variant['beta-Caryophyllene'], 2.5)

    @patch('scrapers.iheartjane_scraper.requests.post')
    def test_fetch_iheartjane_data_flow(self, mock_post):
        """Test the main fetch function."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "products": [self.mock_product_hit_flower, self.mock_product_hit_vape]
        }
        mock_post.return_value = mock_response

        df = fetch_iheartjane_data(1234, "Test Store")

        mock_post.assert_called_once()
        self.assertIsInstance(df, pd.DataFrame)
        # 2 variants from flower + 1 from vape = 3 rows
        self.assertEqual(len(df), 3)
        self.assertEqual(df.iloc[0]['Name'], "Test Flower")
        self.assertEqual(df.iloc[2]['Name'], "Test Vape")

    @patch('scrapers.iheartjane_scraper.requests.post')
    def test_fetch_iheartjane_data_no_data(self, mock_post):
        """Test the fetch function when the API returns no products."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"products": []}
        mock_post.return_value = mock_response

        df = fetch_iheartjane_data(5678, "Empty Store")

        self.assertIsInstance(df, pd.DataFrame)
        self.assertTrue(df.empty)

if __name__ == '__main__':
    unittest.main()
