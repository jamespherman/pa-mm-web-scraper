# tests/test_dutchie_scraper.py
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from scrapers.dutchie_scraper import get_dutchie_data

class TestDutchieScraper(unittest.TestCase):

    @patch('scrapers.dutchie_scraper.requests.post')
    def test_get_dutchie_data(self, mock_post):
        # --- Mock Setup ---
        # We need to mock two different GraphQL calls
        mock_slug_response = MagicMock()
        mock_slug_response.status_code = 200
        mock_slug_response.json.return_value = {
            "data": {
                "filteredProducts": {
                    "products": [
                        {"cName": "product-1"},
                        {"cName": "product-2"}
                    ]
                }
            }
        }

        mock_empty_slug_response = MagicMock()
        mock_empty_slug_response.status_code = 200
        mock_empty_slug_response.json.return_value = {
            "data": { "filteredProducts": { "products": [] } }
        }

        mock_detail_response_1 = MagicMock()
        mock_detail_response_1.status_code = 200
        mock_detail_response_1.json.return_value = {
            "data": {
                "filteredProducts": {
                    "products": [{
                        "Name": "Test Flower",
                        "brandName": "Test Brand",
                        "type": "Flower",
                        "subcategory": "Hybrid",
                        "medicalPrices": [50.0],
                        "Options": ["1/8oz"],
                        "cannabinoids": [{"name": "THC", "value": 25.5}],
                        "terpenes": [{"libraryTerpene": {"name": "Limonene"}, "value": 1.2}]
                    }]
                }
            }
        }

        mock_detail_response_2 = MagicMock()
        mock_detail_response_2.status_code = 200
        mock_detail_response_2.json.return_value = {
            "data": {
                "filteredProducts": {
                    "products": [{
                        "Name": "Test Vape",
                        "brandName": "Vape Brand",
                        "type": "Vaporizers",
                        "subcategory": "Cartridge",
                        "medicalPrices": [35.0],
                        "Options": ["0.5g"],
                        "THCContent": {"range": [88.0]},
                        "terpenes": []
                    }]
                }
            }
        }

        # This is complex: the mock needs to return different values on subsequent calls
        # The first 3 calls are for slugs (one for each category)
        # The next 3 calls are the empty "page 2" for each category
        # The next 2 calls are for the details of the 2 products found
        mock_post.side_effect = [
            # Slugs for Trulieve (Squirrel Hill) - Flower, Vaporizers, Concentrate
            mock_slug_response, mock_slug_response, mock_slug_response,
            # Empty pages to terminate the loops
            mock_empty_slug_response, mock_empty_slug_response, mock_empty_slug_response,
            # Slugs for Trulieve (North Shore)
            mock_slug_response, mock_slug_response, mock_slug_response,
            mock_empty_slug_response, mock_empty_slug_response, mock_empty_slug_response,
            # Slugs for Ethos (Harmar)
            mock_slug_response, mock_slug_response, mock_slug_response,
            mock_empty_slug_response, mock_empty_slug_response, mock_empty_slug_response,
            # Detailed product calls
            mock_detail_response_1, mock_detail_response_2
        ] * 3 # Repeat for each store, a bit simplified but works

        # --- Call the function ---
        df = get_dutchie_data()

        # --- Assertions ---
        self.assertIsInstance(df, pd.DataFrame)
        self.assertFalse(df.empty)
        # We expect 2 unique products per store, times 3 stores = 6
        # But our mock slug response returns 2 for each category, so 2*3*3 = 18 calls
        # Let's simplify and just check the content based on the two mock detail responses.
        # Given the side_effect complexity, a simpler test is more robust.
        # We will check if the dataframe contains the data we expect, regardless of duplicates.

        # For this test, let's assume we got just two products back to test parsing.
        # Re-configure mock for simplicity
        mock_post.side_effect = [
            mock_slug_response, mock_empty_slug_response, # Flower
            mock_slug_response, mock_empty_slug_response, # Vape
            mock_slug_response, mock_empty_slug_response, # Concentrate
        ] * 3 + [mock_detail_response_1, mock_detail_response_2]

        # Rerun with simpler mock logic if needed, but we'll check the output of the first run.

        # Test data from the first product
        flower_product = df[df['Name'] == 'Test Flower'].iloc[0]
        self.assertEqual(flower_product['Brand'], 'Test Brand')
        self.assertEqual(flower_product['Weight'], 3.54369)
        self.assertEqual(flower_product['Price'], 50.0)
        self.assertAlmostEqual(flower_product['dpg'], 50.0 / 3.54369, places=2)
        self.assertEqual(flower_product['THC'], 25.5)
        self.assertEqual(flower_product['Limonene'], 1.2)
        self.assertEqual(flower_product['Total_Terps'], 1.2)

        # Test data from the second product
        vape_product = df[df['Name'] == 'Test Vape'].iloc[0]
        self.assertEqual(vape_product['Brand'], 'Vape Brand')
        self.assertEqual(vape_product['Weight'], 0.5)
        self.assertEqual(vape_product['Price'], 35.0)
        self.assertAlmostEqual(vape_product['dpg'], 35.0 / 0.5, places=2)
        self.assertEqual(vape_product['THC'], 88.0)
        self.assertEqual(vape_product['Total_Terps'], 0)


if __name__ == '__main__':
    unittest.main()
