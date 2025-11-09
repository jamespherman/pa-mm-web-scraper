# scrapers/cresco_scraper.py
# This scraper fetches data from the Cresco/Sunnyside API.

import requests
import pandas as pd
import numpy as np
import time
# Note: This imports from the 'scraper_utils.py' file
from .scraper_utils import convert_to_grams
import re

# --- Constants ---
BASE_URL = "https://api.crescolabs.com/p/inventory/op/fifo-inventory"

# --- HEADERS ---
# These are taken directly from your new network logs.
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

# --- CATEGORY LIST ---
KNOWN_CATEGORIES = [
    'flower',
    'vapes',
    'edibles',
    'concentrates',
    'capsules',
    'tinctures',
    'topicals',
    'accessories',
    'pre-roll',
    'beverage'
]

# --- TERPENE LIST ---
# This list is now based on the *standardized names* we will map to
# from the 'potency' object in the API response.
KNOWN_TERPENES = [
    'alpha-Pinene', 'beta-Pinene', 'Myrcene', 'Limonene', 'Terpinolene',
    'Ocimene', 'Linalool', 'beta-Caryophyllene', 'Humulene', 'Nerolidol',
    'Guaiol', 'alpha-Bisabolol', 'Camphene', 'delta-3-Carene',
    'alpha-Terpinene', 'gamma-Terpinene', 'p-Cymene', 'Eucalyptol',
    'beta-Eudesmol', 'Caryophyllene Oxide', 'Fenchone', 'Borneol'
]

# --- Main Function ---
def get_cresco_data(store_map):
    """
    Fetches and processes product data from the Cresco API for given store IDs.
    
    Args:
        store_map (dict): A dictionary mapping {store_name: store_id}

    Returns:
        pd.DataFrame: A DataFrame containing all processed product data.
    """
    all_products_list = []
    limit = 1000 # Set a very high limit to get all products in one request

    for store_name, store_id in store_map.items():
        print(f"--- Scraping store: {store_name} ({store_id}) ---")
        
        request_headers = HEADERS.copy()
        request_headers['store_id'] = store_id
        
        for category in KNOWN_CATEGORIES:
            print(f"  Fetching category: {category}...")
            
            # --- PARAMS ---
            # We make one request per category with a high limit.
            params = {
                # 'page': 1, # This API does not use 'page'
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
                # --- Make the GET Request ---
                response = requests.get(BASE_URL, headers=request_headers, params=params, timeout=15)
                response.raise_for_status()
                
                data = response.json()
                products = data.get('data', [])

                if not products:
                    print(f"    ...found 0 products for {category}.")
                    continue 

                # --- Process and Add Data ---
                products_df = process_product_data(products, store_name)
                all_products_list.append(products_df)
                
                print(f"    ...found {len(products_df)} products for {category}.")

                # Politeness delay
                time.sleep(0.5)
                
            except requests.exceptions.RequestException as e:
                print(f"Error fetching {category} at {store_name}: {e}")
                continue
            except Exception as e:
                print(f"An error occurred processing {category}: {e}")
                continue
    
    # --- End of loops ---

    if not all_products_list:
        print("No product data was fetched from Cresco. Returning an empty DataFrame.")
        return pd.DataFrame()

    # --- Final DataFrame ---
    df = pd.concat(all_products_list, ignore_index=True)

    # Calculate DPG
    df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
    df['Weight'] = pd.to_numeric(df['Weight'], errors='coerce')
    df['dpg'] = df.apply(lambda row: row['Price'] / row['Weight'] if row['Weight'] > 0 else np.nan, axis=1)
    
    # --- Define column order ---
    # These are the "base" columns
    other_cols = [
        'Name', 'Store', 'Brand', 'Type', 'Subtype', 'Weight', 
        'Weight_Str', 'Price', 'dpg', 'Total_Terps'
    ]
    
    # Find all terpene columns that were actually created
    present_terpenes = [col for col in KNOWN_TERPENES if col in df.columns]
    
    # Cannabinoid cols are whatever is left over (e.g., 'THC', 'THCA', etc.)
    present_cannabinoids = [
        col for col in df.columns if col not in other_cols and col not in present_terpenes
    ]

    # Build the final, sorted list of columns
    column_order = (
        ['Name', 'Store', 'Brand', 'Type', 'Subtype', 'Weight', 'Weight_Str', 'Price', 'dpg', 'Total_Terps'] +
        sorted(present_cannabinoids) +
        sorted(present_terpenes)
    )
    
    # Add any missing columns (e.g., if no products had 'Total_Terps')
    for col in column_order:
        if col not in df:
            df[col] = np.nan
            
    # Reorder and fill NaNs
    df = df.reindex(columns=column_order).fillna(np.nan)

    print(f"\nTotal products processed for all stores: {len(df)}")
    return df

# --- Data Processing Function ---
# THIS FUNCTION IS COMPLETELY REBUILT BASED ON YOUR NEW SNIPPET
def process_product_data(products, store_name):
    """
    Processes a list of raw product dictionaries from the API
    based on the new, correct data structure.
    """
    processed_list = []
    
    # This maps the lowercase API key from the 'potency' object
    # to the standardized name we want in our final DataFrame.
    TERPENE_MAP = {
        'b_caryophyllene': 'beta-Caryophyllene',
        'b_myrcene': 'Myrcene',
        'b_pinene': 'beta-Pinene',
        'bisabolol': 'alpha-Bisabolol',
        'camphene': 'Camphene',
        'carene': 'delta-3-Carene',
        'humulene': 'Humulene',
        'limonene': 'Limonene',
        'linalool': 'Linalool',
        'ocimene': 'Ocimene',
        'pinene': 'alpha-Pinene', # Map 'pinene' to 'alpha-Pinene'
        'terpinolene': 'Terpinolene',
        'guaiol': 'Guaiol',
        'caryophyllene_oxide': 'Caryophyllene Oxide',
        'eucalyptol': 'Eucalyptol',
        'nerolidol': 'Nerolidol' # A guess, based on 'trans_nerolidal'
    }
    
    for product in products:
        # Get nested objects safely
        sku = product.get('sku', {})
        sku_product = sku.get('product', {})
        sku_strain = sku.get('strain', {})
        potency = product.get('potency', {})
        
        # --- Basic Info ---
        data = {
            'Name': product.get('name'),
            'Store': store_name,
            'Brand': product.get('brand'),
            'Type': sku_product.get('category', 'Other'),
            'Subtype': sku_product.get('sub_category'),
            'Weight_Str': sku_product.get('weight'),
            'Weight': convert_to_grams(sku_product.get('weight')),
            # Get discounted price first, fall back to base price
            'Price': product.get('discounted_price') or product.get('price')
        }

        # --- Cannabinoids ---
        # Get from the top-level 'bt_potency' fields
        if product.get('bt_potency_thc') is not None:
            data['THC'] = product.get('bt_potency_thc')
        if product.get('bt_potency_thca') is not None:
            data['THCA'] = product.get('bt_potency_thca')
        if product.get('bt_potency_cbd') is not None:
            data['CBD'] = product.get('bt_potency_cbd')
        if product.get('bt_potency_cbda') is not None:
            data['CBDA'] = product.get('bt_potency_cbda')
        # Add any other 'bt_potency_' fields you want here

        # --- Terpenes ---
        total_terps = 0
        if potency: # Use the 'potency' object
            # Get total terps from one of two fields
            total_terps = potency.get('total_terps') or product.get('bt_potency_terps')
            
            # Map individual terpenes
            for api_name, standard_name in TERPENE_MAP.items():
                if potency.get(api_name) is not None and potency.get(api_name) > 0:
                    data[standard_name] = potency[api_name]
        
        # Fallback in case 'potency' object was missing
        if total_terps == 0 or total_terps is None:
             total_terps = product.get('bt_potency_terps')
        
        data['Total_Terps'] = total_terps if total_terps > 0 else np.nan

        processed_list.append(data)
    
    return pd.DataFrame(processed_list)