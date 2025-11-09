# scrapers2/dutchie_scraper.py
# This scraper fetches data from the Dutchie GraphQL API.

import requests
import pandas as pd
import numpy as np
import json
import time
from .scraper_utils import convert_to_grams

# --- Constants ---

# The DUTCHIE_STORES dictionary is the core configuration for this scraper.
# This version is simplified back to what you provided, removing the
# incorrect 'detail_hash' I added.
DUTCHIE_STORES = {
    "Ethos (Harmar)": {
        "api_url": "https://harmarville.ethoscannabis.com/api-4/graphql",
        "store_id": "621900cebbc5580e15476deb",
        "headers": {
            "accept": "*/*",
            "apollographql-client-name": "Marketplace (production)",
            "content-type": "application/json",
            "referer": "https://harmarville.ethoscannabis.com/stores/ethos-harmarville",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0",
            "x-dutchie-session": "eyJpZCI6ImM1MDc2NGI5LTAyZWUtNDU2ZS05ODc0LTZmNzkyOTQwYzc2NiIsImV4cGlyZXMiOjE3NjI0Mjc5MTU5NzR9"
        }
    },
    "Ethos (Pleasant Hills)": {
        "api_url": "https://pittsburgh.ethoscannabis.com/api-4/graphql",
        "store_id": "607dc27bfde18500b5e8dd52",
        "headers": {
            "accept": "*/*",
            "apollographql-client-name": "Marketplace (production)",
            "content-type": "application/json",
            "referer": "https://pleasanthills.ethoscannabis.com/stores/ethos-pleasant-hills",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0",
            "x-dutchie-session": "eyJpZCI6ImM1MDc2NGI5LTAyZWUtNDU2ZS05ODc0LTZmNzkyOTQwYzc2NiIsImV4cGlyZXMiOjE3NjI0Mjc5MTU5NzR9"
        }
    },
    "Ethos (North Fayette)": {
        "api_url": "https://pittsburgh.ethoscannabis.com/api-4/graphql",
        "store_id": "5fa0829005bb2400cfc4b694",
        "headers": {
            "accept": "*/*",
            "apollographql-client-name": "Marketplace (production)",
            "content-type": "application/json",
            "referer": "https://northfayette.ethoscannabis.com/stores/ethos-north-fayette",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0",
            "x-dutchie-session": "eyJpZCI6ImM1MDc2NGI5LTAyZWUtNDU2ZS05ODc0LTZmNzkyOTQwYzc2NiIsImV4cGlyZXMiOjE3NjI0Mjc5MTU5NzR9"
        }
    },
    # "Liberty (PGH)": { ... }, # This is still commented out as in your example
    "Ascend (Cranberry)": {
        "api_url": "https://letsascend.com/api-4/graphql",
        "store_id": "66fef50576b5d1b3703a1890",
        "headers": {
            "accept": "*/*",
            "apollographql-client-name": "Marketplace (production)",
            "content-type": "application/json",
            "referer": "https://letsascend.com/stores/cranberry-pennsylvania",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15",
            "x-dutchie-session": "eyJpZCI6IjNhMTFmZGZhLTU5MGQtNDk5ZC1hYzE4LTRjNjhlZjRjNjZkNiIsImV4cGlyZXMiOjE3NjI0ODA3NzY0ODF9"
        }
    },
    "Ascend (Monaca)": {
        "api_url": "https://letsascend.com/api-4/graphql",
        "store_id": "66fef58038ff55ae0d700b55",
        "headers": {
            "accept": "*/*",
            "apollographql-client-name": "Marketplace (production)",
            "content-type": "application/json",
            "referer": "https://letsascend.com/stores/monaca-pennsylvania",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15",
            "x-dutchie-session": "eyJpZCI6IjNhMTFmZGZhLTU5MGQtNDk5ZC1hYzE4LTRjNjhlZjRjNjZkNiIsImV4cGlyZXMiOjE3NjI0ODA3NzY0ODF9"
        }
    }
}


# --- GraphQL Query Hashes (from our network inspection) ---
LIST_HASH = "ee29c060826dc41c527e470e9ae502c9b2c169720faa0a9f5d25e1b9a530a4a0"
# This is the single, correct hash from YOUR working snippet
DETAIL_HASH = "47369a02fc8256aaf1ed70d0c958c88514acdf55c5810a5be8e0ee1a19617cda"


def get_all_product_slugs(store_name, store_config):
    """
    Step 1: Fetches the *list* of all products from a single store.
    This function gets the basic info (like the 'cName' slug) for all
    products on the menu.
    """
    print(f"--- Scraping store: {store_name} ---")
    
    product_slugs = []
    page = 0
    products_per_page = 100
    total_found = 0

    while True:
        variables = {
            "includeEnterpriseSpecials": False,
            "productsFilter": {
                "dispensaryId": store_config['store_id'],
                "pricingType": "med",
                "types": [], # Empty list to get all types
                "useCache": False,
                "isDefaultSort": True,
                "sortBy": "weight",
                "sortDirection": 1,
                "removeProductsBelowOptionThresholds": True
            },
            "page": page,
            "perPage": products_per_page
        }
        
        extensions = {
            "persistedQuery": {
                "version": 1,
                "sha256Hash": LIST_HASH
            }
        }
        
        params = {
            'operationName': 'FilteredProducts',
            'variables': json.dumps(variables),
            'extensions': json.dumps(extensions)
        }
        
        try:
            response = requests.get(store_config['api_url'], headers=store_config['headers'], params=params, timeout=10)
            response.raise_for_status()
            
            json_response = response.json()
            
            if 'errors' in json_response:
                print(f"  GraphQL Error in product list: {json_response['errors']}")
                break
            
            products = json_response['data']['filteredProducts']['products']
            
            if not products:
                print(f"  ...completed store. Found {total_found} total products.")
                break
                
            print(f"  Page {page}: Found {len(products)} products.")
            
            for product in products:
                # Add store context for the detail fetcher
                product['StoreName'] = store_name
                product['StoreConfig'] = store_config
            
            product_slugs.extend(products)
            
            total_found += len(products)
            page += 1
            time.sleep(0.1) # Politeness delay
            
        except requests.exceptions.RequestException as e:
            print(f"  Error fetching page {page} for {store_name}: {e}")
            break
        except KeyError:
            print(f"  Unexpected JSON structure on page {page} for {store_name}. Skipping.")
            print(f"  Response was: {response.text}")
            break
            
    return product_slugs


def _fetch_product_details(unique_products):
    """
    Step 2: Fetches the *raw detailed JSON* for each unique product.
    This function uses the logic from YOUR working snippet.
    """
    all_product_details = []
    print(f"\nFetching detailed info for all {len(unique_products)} unique products...")

    for i, slug_info in enumerate(unique_products):
        # These are the keys from the 'product_slugs' list
        cName = slug_info['cName']
        dispensaryId = slug_info['DispensaryID']
        store_name = slug_info['StoreName']
        store_config = slug_info['StoreConfig']
        
        # This is the exact logic from your working snippet
        variables = {
            "includeTerpenes": True,
            "includeCannabinoids": True,
            "includeEnterpriseSpecials": False,
            "productsFilter": {
                "cName": cName,
                "dispensaryId": dispensaryId,
                "removeProductsBelowOptionThresholds": False,
                "isKioskMenu": False,
                "bypassKioskThresholds": False,
                "bypassOnlineThresholds": True,
                "Status": "All"
            }
        }
        
        extensions = {
            "persistedQuery": {
                "version": 1,
                "sha256Hash": DETAIL_HASH # Use the correct, single hash
            }
        }
        
        params = {
            'operationName': 'IndividualFilteredProduct',
            'variables': json.dumps(variables),
            'extensions': json.dumps(extensions)
        }
        
        if (i + 1) % 25 == 0:
             print(f"  Fetching detail for product {i + 1} of {len(unique_products)} ({cName})...")

        try:
            response = requests.get(store_config['api_url'], headers=store_config['headers'], params=params, timeout=10)
            response.raise_for_status()
            
            json_response = response.json()
            
            if 'errors' in json_response:
                print(f"  GraphQL Error for {cName}: {json_response['errors']}")
                continue
                
            products = json_response['data']['filteredProducts']['products']

            if not products:
                print(f"  No product data found in response for {cName}")
                continue

            # Success! Get the detailed product object
            product_detail = products[0]
            
            # Add our store_name context for the parser
            product_detail['StoreName'] = store_name
            all_product_details.append(product_detail)
            
            time.sleep(0.1) # Politeness delay

        except requests.exceptions.RequestException as e:
            print(f"  Error fetching details for {cName}: {e}")
        except KeyError:
            print(f"  Unexpected JSON structure for details {cName}. Skipping.")
            print(f"  Response was: {response.text}")
            continue

    print(f"\nSuccessfully fetched details for {len(all_product_details)} products.")
    return all_product_details


def get_dutchie_data():
    """
    Main function to run the full Dutchie scrape.
    1. Gets all product slugs (list view).
    2. Gets unique product slugs.
    3. Fetches detailed info for each unique slug.
    4. Returns the raw, detailed JSON list.
    """
    all_store_slugs = []
    
    # Step 1: Get product slugs from all stores
    for store_name, store_config in DUTCHIE_STORES.items():
        slugs = get_all_product_slugs(store_name, store_config)
        all_store_slugs.extend(slugs)

    if not all_store_slugs:
        print("No product slugs found for any Dutchie store.")
        return []

    # Step 2: Get unique products
    # We use 'cName' as the unique ID
    unique_products = {p['cName']: p for p in all_store_slugs}.values()
    print(f"\nFound {len(all_store_slugs)} total product listings, representing {len(unique_products)} unique products.")

    # Step 3: Fetch detailed info for each unique product
    raw_detailed_products = _fetch_product_details(list(unique_products))

    return raw_detailed_products
