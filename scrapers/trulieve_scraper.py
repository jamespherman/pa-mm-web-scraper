# scrapers/prulieve_scraper.py
# This scraper fetches data from the Trulieve v2 API.

import requests
import pandas as pd
import numpy as np
import time  # Import time for the sleep delay
# Note: This imports from the 'scraper_utils.py' file
from .scraper_utils import convert_to_grams
import re

# --- Constants ---
# This URL is correct
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

# This list is also correct
CATEGORIES = [
    "flower", 
    "vapes", 
    "concentrates", 
    "tinctures", 
    "edibles",
    "accessories",
    "pre-roll"
]

# Define known terpenes to look for
KNOWN_TERPENES = [
    'alpha-Pinene', 'beta-Pinene', 'Myrcene', 'Limonene', 'Terpinolene',
    'Ocimene', 'Linalool', 'beta-Caryophyllene', 'Humulene', 'Nerolidol',
    'Bisabolol', 'CaryophylleneOxide' # Added from your snippet
]

# --- Main Function ---
def get_trulieve_data(store_map):
    """
    Fetches and processes product data from the Trulieve API for given store IDs.
    
    Args:
        store_map (dict): A dictionary mapping {store_name: store_id}

    Returns:
        pd.DataFrame: A DataFrame containing all processed product data.
    """
    all_products_list = []
    
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
                    'search': "",
                    'weights': "",
                    'brand': "",
                    'strain_type': "",
                    'subcategory': "",
                    'cbd_max': "",
                    'cbd_min': "",
                    'thc_max': "",
                    'thc_min': "",
                    'special': "",
                    'sort_by': "default",
                }

                try:
                    response = requests.get(url, headers=HEADERS, params=params, timeout=10)
                    response.raise_for_status()
                    
                    data = response.json()
                    
                    # --- THIS IS THE FIRST FIX ---
                    # The product list is under the "data" key, not "products"
                    products = data.get('data', []) 

                    if not products:
                        if page == 1:
                            print(f"    ...found 0 products for {category}.")
                        else:
                            print(f"  ...completed category: {category}. Found {total_scraped} products.")
                        break 
                        
                    print(f"    Page {page}: Found {len(products)} products.")

                    # --- THIS IS THE SECOND FIX ---
                    # We pass the new, correct data structure to the parser
                    products_df = process_product_data(products, store_name)
                    all_products_list.append(products_df)
                    total_scraped += len(products_df)
                    
                    time.sleep(0.5) 
                        
                    # Check for the last page
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

    if not all_products_list:
        print("No product data was fetched from Trulieve. Returning an empty DataFrame.")
        return pd.DataFrame()

    # --- Final DataFrame ---
    df = pd.concat(all_products_list, ignore_index=True)

    # Calculate DPG
    df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
    df['Weight'] = pd.to_numeric(df['Weight'], errors='coerce')
    df['dpg'] = df.apply(lambda row: row['Price'] / row['Weight'] if row['Weight'] > 0 else np.nan, axis=1)
    
    # Define column order
    base_cols = ['Name', 'Store', 'Brand', 'Type', 'Subtype', 'Weight', 'Weight_Str', 'Price', 'dpg', 'Total_Terps']
    
    # Dynamically get all cannabinoid columns (all-caps)
    cannabinoid_cols = sorted([col for col in df.columns if col.isupper() and len(col) <= 5 and col not in base_cols])
    # Dynamically get all terpene columns
    terpene_cols = sorted([col for col in df.columns if col in KNOWN_TERPENES])

    column_order = base_cols + cannabinoid_cols + terpene_cols
    
    # Add any missing columns
    for col in column_order:
        if col not in df:
            df[col] = np.nan
            
    # Reorder and fill NaNs
    df = df.reindex(columns=column_order).fillna(np.nan)

    print(f"\nTotal products processed for all stores: {len(df)}")
    return df


# --- Data Processing Function ---
def process_product_data(products, store_name):
    """
    Processes a list of raw product dictionaries from the Trulieve API
    based on the new, correct data structure from your snippet.
    """
    processed_list = []
    
    for product in products:
        
        # Extract variant info (for price and weight)
        # Use .get('variants', [{}])[0] to safely get the first variant or an empty dict
        variant = product.get('variants', [{}])[0]
        if not variant:
            variant = {} # Ensure variant is a dict if the list was empty
            
        weight_str = variant.get('option')
        
        # Basic Info
        data = {
            'Name': product.get('name'),
            'Store': store_name,
            'Brand': product.get('brand'),
            'Type': product.get('category'),
            'Subtype': product.get('subcategory'),
            'Weight_Str': weight_str,
            'Weight': convert_to_grams(weight_str),
            'Price': variant.get('sale_unit_price') or variant.get('unit_price') or product.get('unit_price')
        }

        # Cannabinoids (from top-level keys)
        if product.get('thc_content') is not None:
            data['THC'] = product.get('thc_content')
        if product.get('cbd_content') is not None:
            data['CBD'] = product.get('cbd_content')
        # Add any others you see, e.g.:
        # data['CBN'] = product.get('cbn_content') 

        # Terpenes (from the 'terpenes' list)
        terpenes = product.get('terpenes', [])
        total_terps = 0
        if terpenes:
            for t in terpenes:
                name = t.get('name')
                value = t.get('value')
                
                # Normalize the name from the API (e.g., "BetaCaryophyllene")
                # to our standard (e.g., "beta-Caryophyllene")
                if name == "BetaCaryophyllene":
                    clean_name = "beta-Caryophyllene"
                elif name == "BetaMyrcene":
                    clean_name = "Myrcene"
                elif name == "BetaPinene":
                    clean_name = "beta-Pinene"
                elif name == "Pinene": # Assuming this is alpha
                    clean_name = "alpha-Pinene"
                elif name == "Bisabolol":
                    clean_name = "Bisabolol"
                elif name == "CaryophylleneOxide":
                    clean_name = "CaryophylleneOxide"
                elif name == "Humulene":
                    clean_name = "Humulene"
                elif name == "Limonene":
                    clean_name = "Limonene"
                elif name == "Linalool":
                    clean_name = "Linalool"
                elif name == "Terpinolene":
                    clean_name = "Terpinolene"
                elif name == "Ocimene":
                    clean_name = "Ocimene"
                else:
                    clean_name = name # Keep it if we don't have a map
                
                if value is not None:
                    if clean_name in KNOWN_TERPENES:
                        data[clean_name] = value
                    total_terps += value
        
        data['Total_Terps'] = total_terps if total_terps > 0 else np.nan

        processed_list.append(data)
    
    return pd.DataFrame(processed_list)