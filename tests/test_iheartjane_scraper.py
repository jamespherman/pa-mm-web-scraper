# tests/test_iheartjane_scraper.py
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from scrapers.iheartjane_scraper import fetch_iheartjane_data

class TestIHeartJaneScraper(unittest.TestCase):

    @patch('scrapers.iheartjane_scraper.requests.post')
    def test_fetch_iheartjane_data_single_call(self, mock_post):
        # Create a mock response object
        mock_response = MagicMock()
        mock_response.status_code = 200

        # This is a sample of the JSON data we expect from the API
        mock_response.json.return_value = {
            "products": [
                {
                    "search_attributes": {
                        "name": "Test Product 1",
                        "brand": "Test Brand",
                        "kind": "Flower",
                        "kind_subtype": "Indica",
                        "percent_thc": 20.5,
                        "available_weights": ["gram"],
                        "price_gram": 15.0,
                        "store_notes": "Terpene Profile: beta-Myrcene: 1.2%, Limonene 0.8%",
                        "inventory_potencies": [
                            {
                                "price_id": "gram",
                                "thca_potency": 22.5,
                                "cbd_potency": 0.5
                            }
                        ]
                    }
                },
                {
                    "search_attributes": {
                        "name": "Test Product 2",
                        "brand": "Test Brand",
                        "kind": "Vape",
                        "kind_subtype": "Sativa",
                        "percent_thc": 85.0,
                        "price_each": 50.0,
                        "description": "b-Caryophyllene: 2.5%",
                        "inventory_potencies": [
                            {
                                "price_id": "each",
                                "thca_potency": 90.0,
                                "cbg_potency": 2.0
                            }
                        ]
                    }
                }
            ]
        }
        mock_post.return_value = mock_response

        # Call the function
        store_id = 1234
        store_name = "Test Store"
        df = fetch_iheartjane_data(store_id, store_name)

        # 1. Assert that requests.post was called exactly once
        mock_post.assert_called_once()

        # 2. Assert that the returned object is a DataFrame
        self.assertIsInstance(df, pd.DataFrame)

        # 3. Assert that the DataFrame is not empty
        self.assertFalse(df.empty)

        # 4. Assert that we have the correct number of product variants
        self.assertEqual(len(df), 2)

        # 5. Spot-check some of the data
        self.assertEqual(df.iloc[0]['Name'], "Test Product 1")
        self.assertEqual(df.iloc[1]['Price'], 50.0)

        # 6. Test the new parsing logic
        self.assertEqual(df.iloc[0]['THCa'], 22.5)
        self.assertEqual(df.iloc[0]['CBD'], 0.5)
        self.assertEqual(df.iloc[0]['beta-Myrcene'], 1.2)
        self.assertEqual(df.iloc[0]['Limonene'], 0.8)
        self.assertEqual(df.iloc[1]['beta-Caryophyllene'], 2.5)
        self.assertEqual(df.iloc[1]['CBG'], 2.0)


if __name__ == '__main__':
    unittest.main()
