# scrapers2/iheartjane_scraper.py
# This scraper's *only* job is to fetch raw data from the iHeartJane v2 API.
# All processing is handled in 'parse_iheartjane.py'.

import requests
import json
import time

# --- API Constants ---
# These are the constants YOU provided and confirmed are working.
NEW_JANE_URL = "https://dmerch.iheartjane.com/v2/smart"
NEW_JANE_API_KEY = "ce5f15c9-3d09-441d-9bfd-26e87aff5925"

# --- HEADERS ---
# We still need to send browser-like headers to avoid being blocked.
HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "en-US,en;q=0.9",
    "origin": "https://www.iheartjane.com",
    "referer": "https://www.iheartjane.com/",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15",
}

# This list of facets is from your working snippet.
FULL_SEARCH_FACETS = [
    "activities", "aggregate_rating", "applicable_special_ids",
    "available_weights", "brand_subtype", "brand", "bucket_price",
    "category", "feelings", "has_brand_discount", "kind",
    "percent_cbd", "percent_thc", "root_types", "compound_names"
]

# --- Main Scraper Function ---
def get_jane_data(store_map):
    """
    Fetches raw product data from the iHeartJane API for given store IDs.
    
    Args:
        store_map (dict): A dictionary mapping {store_name: store_id}

    Returns:
        list: A list of all raw product dictionaries.
    """
    all_products_list = []
    
    # --- THIS IS THE FIX ---
    # These are the correct parameters from your working script snippet
    params = {
        'jdm_api_key': NEW_JANE_API_KEY,
        'jdm_source': 'monolith',
        'jdm_version': '2.12.0'
    }

    for store_name, store_id in store_map.items():
        print(f"--- Scraping store: {store_name} ({store_id}) ---")
        
        # This payload tells the API to get *all* products from a specific store
        payload = {
            "app_mode": "embedded",
            "jane_device_id": "me7dtQx8hW9YlcYmnHPys", # Static ID from your snippet
            "search_attributes": ["*"],
            "store_id": store_id,
            "disable_ads": False,
            "num_columns": 1,
            "page_size": 1000, # Request a large page size to get all products
            "page": 0,
            "placement": "menu_inline_table",
            "search_facets": FULL_SEARCH_FACETS, # Use the full list
            "search_filter": f"store_id = {store_id}",
            "search_query": "", # Empty query to get ALL products
            "search_sort": "recommendation"
        }

        try:
            # Make the POST request, *now with correct params and headers*
            response = requests.post(NEW_JANE_URL, params=params, headers=HEADERS, json=payload, timeout=20)
            response.raise_for_status() # Raise an error for bad responses
            
            data = response.json()
            # The raw product data is in the 'products' key
            hits = data.get('products', [])

            print(f"  ...retrieved {len(hits)} products in a single call.")

            # Add store_name to each product for context
            for hit in hits:
                hit['store_name_scraped'] = store_name
            
            all_products_list.extend(hits)
            
            # Politeness delay
            time.sleep(1)
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for store {store_id}: {e}")
        except json.JSONDecodeError:
            print(f"Failed to decode JSON response for store {store_id}.")
            
    return all_products_list
