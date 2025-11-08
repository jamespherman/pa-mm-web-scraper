# [START SCRIPT]
# --- Save as: comprehensive_profiler.py ---
# This is a standalone, one-off script to
# comprehensively profile all scraper APIs.
# This version *correctly* imports from main.py.

import requests
import json
import re
import time
import pandas as pd # Added for cleaning
import numpy as np  # Added for cleaning

# --- Scraper-Specific Imports (Corrected) ---

# Import Store Dictionaries from main.py
from main import CRESCO_STORES, TRULIEVE_STORES, IHEARTJANE_STORES

# Import scraper-specific configs
from scrapers.cresco_scraper import HEADERS as CRESCO_HEADERS
from scrapers.cresco_scraper import CATEGORIES as CRESCO_CATEGORIES
from scrapers.dutchie_scraper import DUTCHIE_STORES
from scrapers.trulieve_scraper import HEADERS as TRULIEVE_HEADERS
from scrapers.trulieve_scraper import CATEGORIES as TRULIEVE_CATEGORIES
from scrapers.iheartjane_scraper import NEW_JANE_URL, NEW_JANE_API_KEY

# --- Global Sets to Store Unique Names ---
all_terpene_names = set()
all_brand_names = set()
all_category_names = set()
all_subcategory_names = set()

def profile_dutchie():
    """Fetches ALL products from Dutchie to profile its names."""
    print("\n--- Profiling Dutchie (Comprehensive) ---")
    
    for store_name, store_config in DUTCHIE_STORES.items():
        print(f"  Profiling Dutchie store: {store_name}...")
        api_url = store_config['api_url']
        store_id = store_config['store_id']
        headers = store_config['headers']
        page = 0
        
        while True:
            try:
                variables = {
                    "includeEnterpriseSpecials": False,
                    "productsFilter": {"dispensaryId": store_id, "pricingType": "med", "Status": "Active"},
                    "page": page,
                    "perPage": 1000 # Fetch 1000 at a time
                }
                extensions = {
                    "persistedQuery": {"version": 1, "sha256Hash": "ee29c060826dc41c527e470e9ae502c9b2c169720faa0a9f5d25e1b9a530a4a0"}
                }
                params = {'operationName': 'FilteredProducts', 'variables': json.dumps(variables), 'extensions': json.dumps(extensions)}
                
                response = requests.get(api_url, headers=headers, params=params, timeout=20)
                response.raise_for_status()
                products = response.json()['data']['filteredProducts']['products']
                
                if not products:
                    break # No more products for this store

                for prod in products:
                    all_brand_names.add(prod.get('brandName'))
                    all_category_names.add(prod.get('type'))
                    all_subcategory_names.add(prod.get('subcategory'))
                    
                    if prod.get('terpenes'):
                        for terp in prod['terpenes']:
                            if terp.get('libraryTerpene'):
                                all_terpene_names.add(terp['libraryTerpene'].get('name'))
                
                print(f"    ... found {len(products)} products on page {page}")
                page += 1
                time.sleep(0.5) # Be nice to the server

            except Exception as e:
                print(f"    ERROR profiling {store_name} on page {page}: {e}")
                break # Stop this store on error
                
    print("Dutchie profile complete.")

def profile_cresco():
    """Fetches ALL products from Cresco to profile its names."""
    print("\n--- Profiling Cresco (Comprehensive) ---")
    
    for store_name, store_id in CRESCO_STORES.items():
        print(f"  Profiling Cresco store: {store_name}...")
        headers = CRESCO_HEADERS.copy()
        headers['store_id'] = store_id
        
        for category in CRESCO_CATEGORIES:
            page = 0
            while True:
                try:
                    params = {
                        'category': category,
                        'inventory_type': 'retail',
                        'require_sellable_quantity': 'true',
                        'include_specials': 'true',
                        'sellable': 'true',
                        'order_by': 'brand',
                        'limit': '1000',
                        'usage_type': 'medical',
                        'hob_first': 'true',
                        'include_filters': 'true',
                        'include_facets': 'true',
                        'offset': str(page * 1000)
                    }
                    
                    response = requests.get("https://api.crescolabs.com/p/inventory/op/fifo-inventory", headers=headers, params=params, timeout=20)
                    response.raise_for_status()
                    products = response.json().get('data', [])

                    if not products:
                        break # No more products

                    for prod in products:
                        all_brand_names.add(prod.get('brand'))
                        all_category_names.add(prod.get('category'))
                        
                        try:
                            # Try to get sub_category from the nested structure
                            all_subcategory_names.add(prod['sku']['product']['sub_category'])
                        except KeyError:
                            pass # No subcategory
                        
                        if prod.get('terpenes'):
                            for terp in prod['terpenes']:
                                all_terpene_names.add(terp.get('terpene')) # e.g., 'beta_myrcene'
                        
                        potency = prod.get('potency')
                        if potency:
                            for key in potency.keys():
                                # Grab all keys from potency dict as potential terp names
                                if key not in ['thc', 'thca', 'cbd', 'cbda', 'total_terps', 'thc_total', 'cbd_total', 'id', 'location_id', 'package_number', 'trans_nerolidal']:
                                    all_terpene_names.add(key)
                    
                    print(f"    ... found {len(products)} products in {category} on page {page}")
                    
                    if len(products) < 50:
                        break # Last page
                        
                    page += 1
                    time.sleep(0.5)

                except Exception as e:
                    print(f"    ERROR profiling {store_name} ({category}) on page {page}: {e}")
                    break # Stop this category on error

    print("Cresco profile complete.")

def profile_trulieve():
    """Fetches ALL products from Trulieve to profile its names."""
    print("\n--- Profiling Trulieve (Comprehensive) ---")
    
    base_url = "https://api.trulieve.com/api/v2/menu/{store_id}/{category}/MEDICAL"
    
    for store_name, store_id in TRULIEVE_STORES.items():
        print(f"  Profiling Trulieve store: {store_name}...")
        for category in TRULIEVE_CATEGORIES:
            page = 1
            while True:
                try:
                    url = f"{base_url.format(store_id=store_id, category=category)}?page={page}"
                    response = requests.get(url, headers=TRULIEVE_HEADERS, timeout=20)
                    response.raise_for_status()
                    
                    json_response = response.json()
                    products = json_response.get('data', [])
                    
                    if not products:
                        break # No more products

                    for prod in products:
                        all_brand_names.add(prod.get('brand'))
                        all_category_names.add(prod.get('category'))
                        all_subcategory_names.add(prod.get('subcategory'))
                        
                        if prod.get('terpenes'):
                            for terp in prod['terpenes']:
                                all_terpene_names.add(terp.get('name')) # e.g., 'BetaCaryophyllene'

                    print(f"    ... found {len(products)} products in {category} on page {page}")
                    
                    # Check pagination
                    last_page = json_response.get('last_page')
                    current_page = json_response.get('current_page')
                    if last_page is not None and current_page is not None and current_page >= last_page:
                        break # Reached the last page
                    
                    page += 1
                    time.sleep(0.5)

                except Exception as e:
                    print(f"    ERROR profiling {store_name} ({category}) on page {page}: {e}")
                    break

    print("Trulieve profile complete.")

def profile_iheartjane():
    """Fetches ALL products from iHeartJane to profile its names."""
    print("\n--- Profiling iHeartJane (Comprehensive) ---")
    
    # This is the FULL payload from the real scraper, which fixes the 400 error
    base_payload = {
        "app_mode": "embedded",
        "jane_device_id": "me7dtQx8hW9YlcYmnHPys",
        "search_attributes": ["*"],
        "disable_ads": False,
        "num_columns": 1,
        "page_size": 60, # We'll paginate with this
        "placement": "menu_inline_table",
        "search_facets": [
            "activities", "aggregate_rating", "applicable_special_ids",
            "available_weights", "brand_subtype", "brand", "bucket_price",
            "category", "feelings", "has_brand_discount", "kind",
            "percent_cbd", "percent_thc", "root_types", "compound_names"
        ],
        "search_query": "",
        "search_sort": "recommendation"
    }
    
    for store_name, store_id in IHEARTJANE_STORES.items():
        print(f"  Profiling iHeartJane store: {store_name}...")
        try:
            payload = base_payload.copy()
            payload["store_id"] = store_id
            payload["page"] = 0
            payload["search_filter"] = f"store_id = {store_id}"
            
            params = {
                'jdm_api_key': NEW_JANE_API_KEY,
                'jdm_source': 'monolith',
                'jdm_version': '2.12.0'
            }
            
            response = requests.post(NEW_JANE_URL, params=params, json=payload, timeout=20)
            response.raise_for_status()
            products = response.json().get('products', [])
            
            if not products:
                break # No more products

            for prod in products:
                attrs = prod.get('search_attributes', {})
                all_brand_names.add(attrs.get('brand'))
                all_category_names.add(attrs.get('kind'))
                all_subcategory_names.add(attrs.get('kind_subtype'))
                
                # iHeartJane is special, we parse from raw text
                raw_text = (attrs.get('store_notes', '') or '') + (attrs.get('description', '') or '')
                if raw_text:
                    # Find all "Name : 1.234%" patterns
                    matches = re.findall(r"([a-zA-Z\s\d\.-]+)[\s:]*([\d\.]+)%", raw_text, re.IGNORECASE)
                    for name, value in matches:
                        name = name.strip()
                        # Filter out non-terpene names
                        if name.lower() not in ['thc', 'cbd', 'thca', 'cbg', 'cbn', 'd8', 'd9'] and '%' not in name:
                            all_terpene_names.add(name) # e.g., 'Caryophyllene', 'b-Pinene'
            
            print(f"    ... found {len(products)} products in a single call.")

        except Exception as e:
            print(f"    ERROR profiling {store_name}: {e}")
            break

    print("iHeartJane profile complete.")

def save_and_print_results():
    """Saves all found unique names to a text file."""
    print("\n--- All Profiling Complete ---")
    
    # Helper to clean up sets
    def clean_and_sort(s):
        # Convert all items to string, filter out None/empty, strip whitespace, and sort
        return sorted(list(set(str(item).strip() for item in s if item is not None and str(item).strip())))

    # Cleaned lists
    terpenes = clean_and_sort(all_terpene_names)
    brands = clean_and_sort(all_brand_names)
    categories = clean_and_sort(all_category_names)
    subcategories = clean_and_sort(all_subcategory_names)

    output_filename = "profiling_output.txt"
    try:
        with open(output_filename, 'w', encoding='utf-8') as f:
            f.write("--- COMPREHENSIVE PROFILING RESULTS ---\n\n")
            
            f.write(f"--- All Terpene Names Found ({len(terpenes)}) ---\n")
            for item in terpenes: f.write(f"{item}\n")
            
            f.write(f"\n--- All Brand Names Found ({len(brands)}) ---\n")
            for item in brands: f.write(f"{item}\n")
            
            f.write(f"\n--- All Category Names Found ({len(categories)}) ---\n")
            for item in categories: f.write(f"{item}\n")
            
            f.write(f"\n--- All Subcategory Names Found ({len(subcategories)}) ---\n")
            for item in subcategories: f.write(f"{item}\n")
        
        print(f"\nSUCCESS: All comprehensive names saved to '{output_filename}'")
        print("\n--- Summary of Categories Found ---")
        print(categories)
        print("\n--- Summary of Subcategories Found ---")
        print(subcategories)
        print("\n--- Summary of Terpene Names Found ---")
        print(terpenes)
        
    except Exception as e:
        print(f"Error saving results to file: {e}")

if __name__ == "__main__":
    profile_dutchie()
    profile_cresco()
    profile_trulieve()
    profile_iheartjane()
    save_and_print_results()

# --- END OF SCRIPT ---
