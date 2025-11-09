# scrapers2/cresco_scraper.py
# This scraper's *only* job is to fetch raw data from the Cresco/Sunnyside API.
# All processing is handled in 'parse_cresco.py'.

import requests
import time
import json # Only used for error logging

# --- Constants ---
BASE_URL = "https://api.crescolabs.com/p/inventory/op/fifo-inventory"

HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip, deflate, br",
    "accept-language": "en-US,en;q=0.9",
    "ordering_app_id": "9ha3c289-1260-4he2-nm62-4598bca34naa",
    "origin": "https://www.sunnyside.shop",
    "referer": "https://www.sunnyside.shop/",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "cross-site",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15",
    "x-api-key": "hE1gQuwYcO54382jYNH0c9W0w4fEC3dJ8ljnwVau",
    "x-client-version": "4.20.0",
    "store_id": "" # This will be set dynamically in the loop
}

KNOWN_CATEGORIES = [
    'flower', 'vapes', 'edibles', 'concentrates', 'capsules',
    'tinctures', 'topicals', 'accessories', 'pre-roll', 'beverage'
]

# --- Main Function ---
def get_cresco_data(store_map):
    """
    Fetches raw product data from the Cresco API for given store IDs.
    
    Args:
        store_map (dict): A dictionary mapping {store_name: store_id}

    Returns:
        list: A list of all raw product dictionaries.
    """
    all_raw_products = []
    limit = 1000 # Set a high limit to get all products in one request per category

    for store_name, store_id in store_map.items():
        print(f"--- Scraping store: {store_name} ({store_id}) ---")
        
        request_headers = HEADERS.copy()
        request_headers['store_id'] = store_id
        
        for category in KNOWN_CATEGORIES:
            print(f"  Fetching category: {category}...")
            
            params = {
                'limit': limit,
                'category': category,
                'inventory_type': 'retail',
                'require_sellable_quantity': 'true',
                'include_specials': 'true',
                'sellable': 'true',
                'order_by': 'brand',
                'usage_type': 'medical',
                'hob_first': 'true',
                'include_filters': 'true',
                'include_facets': 'true'
            }

            try:
                response = requests.get(BASE_URL, headers=request_headers, params=params, timeout=15)
                response.raise_for_status()
                
                data = response.json()
                products = data.get('data', [])

                if not products:
                    print(f"    ...found 0 products for {category}.")
                    continue 

                # Add store_name to each product for context before appending
                for product in products:
                    product['store_name_scraped'] = store_name
                
                all_raw_products.extend(products)
                
                print(f"    ...found {len(products)} products for {category}.")
                time.sleep(0.5)
                
            except requests.exceptions.RequestException as e:
                print(f"Error fetching {category} at {store_name}: {e}")
                continue
            except Exception as e:
                print(f"An error occurred processing {category}: {e}")
                continue
    
    return all_raw_products