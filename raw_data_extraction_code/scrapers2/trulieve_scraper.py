# scrapers2/trulieve_scraper.py
# This scraper's *only* job is to fetch raw data from the Trulieve v2 API.
# All processing is handled in 'parse_trulieve.py'.

import requests
import time
import json # Only used for error logging

# --- Constants ---
BASE_URL = "https://api.trulieve.com/api/v2/menu/{store_id}/{category}/DEFAULT"

HEADERS = {
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.9",
    "origin": "https://www.trulieve.com",
    "referer": "https://www.trulieve.com/",
    "sec-ch-ua": "\"Microsoft Edge\";v=\"141\", \"Not?A_Brand\";v=\"8\", \"Chromium\";v=\"141\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\"",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0"
}

CATEGORIES = [
    "flower", "vapes", "concentrates", "tinctures", 
    "edibles", "accessories", "pre-roll"
]

# --- Main Function ---
def get_trulieve_data(store_map):
    """
    Fetches raw product data from the Trulieve API for given store IDs.
    
    Args:
        store_map (dict): A dictionary mapping {store_name: store_id}

    Returns:
        list: A list of all raw product dictionaries.
    """
    all_raw_products = []
    
    for store_name, store_id in store_map.items():
        print(f"--- Scraping store: {store_name} ({store_id}) ---")
        
        for category in CATEGORIES:
            print(f"  Fetching category: {category}...")
            page = 1
            total_scraped = 0
            
            while True:
                url = BASE_URL.format(store_id=store_id, category=category)
                
                params = {
                    'page': page,
                    'search': "", 'weights': "", 'brand': "", 'strain_type': "",
                    'subcategory': "", 'cbd_max': "", 'cbd_min': "", 'thc_max': "",
                    'thc_min': "", 'special': "", 'sort_by': "default",
                }

                try:
                    response = requests.get(url, headers=HEADERS, params=params, timeout=10)
                    response.raise_for_status()
                    
                    data = response.json()
                    products = data.get('data', []) 

                    if not products:
                        if page == 1:
                            print(f"    ...found 0 products for {category}.")
                        else:
                            print(f"  ...completed category: {category}. Found {total_scraped} products.")
                        break 
                        
                    print(f"    Page {page}: Found {len(products)} products.")

                    # Add store_name to each product for context
                    for product in products:
                        product['store_name_scraped'] = store_name
                        
                    all_raw_products.extend(products)
                    total_scraped += len(products)
                    
                    time.sleep(0.5) 
                        
                    if not data.get('next_page_url'):
                        print(f"  ...completed category: {category}. Found {total_scraped} products.")
                        break
                        
                    page += 1
                    
                except requests.exceptions.RequestException as e:
                    if e.response is not None and e.response.status_code == 404:
                        print(f"    ...category '{category}' does not exist at this store.")
                    else:
                        print(f"Error fetching page {page} for {category} at {store_name}: {e}")
                    break 
                except Exception as e:
                    print(f"An error occurred processing page {page} for {category}: {e}")
                    break

    return all_raw_products