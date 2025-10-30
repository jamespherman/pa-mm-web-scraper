# scrapers/cresco_scraper.py
# This scraper fetches data from the Cresco/Sunnyside API.

import requests
import pandas as pd
import numpy as np
from .scraper_utils import convert_to_grams
import re

# --- Constants ---
BASE_URL = "https://api.crescolabs.com/p/inventory/op/fifo-inventory"

# Headers discovered from browser inspection.
# We are *intentionally* omitting the 'authorization' token, as it is
# user-specific and expires quickly. The 'x-api-key' is the stable key.
HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-US,en;q=0.9,it-IT;q=0.8,it;q=0.7",
    "ordering_app_id": "9ha3c289-1260-4he2-nm62-4598bca34naa",
    "origin": "https://www.sunnyside.shop",
    "referer": "https://www.sunnyside.shop/",
    "sec-ch-ua": "\"Microsoft Edge\";v=\"141\", \"Not?A_Brand\";v=\"8\", \"Chromium\";v=\"141\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\"",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "cross-site",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0",
    "x-api-key": "hE1gQuwYcO54382jYNH0c9W0w4fEC3dJ8ljnwVau",
    "x-client-version": "4.20.0"
}

# Define store configurations
STORES = {
    "Sunnyside (PGH)": "203"
    # We can add more stores here later using their store_id
}

CATEGORIES = ["flower", "vapes", "concentrates"]

KNOWN_TERPENES = [
    'beta-Myrcene', 'Limonene', 'beta-Caryophyllene', 'Terpinolene',
    'Linalool', 'alpha-Pinene', 'beta-Pinene', 'Caryophyllene Oxide',
    'Guaiol', 'Humulene', 'alpha-Bisabolol', 'Camphene', 'Ocimene'
]

# Map API terpene names (e.g., 'beta_myrcene') to our standard names
TERPENE_MAPPING = {
    'alpha_pinene': 'alpha-Pinene',
    'beta_pinene': 'beta-Pinene',
    'beta_myrcene': 'beta-Myrcene',
    'limonene': 'Limonene',
    'beta_caryophyllene': 'beta-Caryophyllene',
    'linalool': 'Linalool',
    'terpinolene': 'Terpinolene',
    'humulene': 'Humulene',
    'ocimene': 'Ocimene',
    'guaiol': 'Guaiol',
    'bisabolol': 'alpha-Bisabolol', # API uses 'bisabolol'
    'camphene': 'Camphene',
    'caryophyllene_oxide': 'Caryophyllene Oxide',
}

def extract_weight_from_cresco_name(name):
    """
    Extracts weight in grams from a product name string (e.g., "3.5g", "1g").
    Uses regex to find patterns like '3.5g', '1g', '500mg'.
    """
    name_lower = name.lower()
    
    # Pattern for 'g'
    g_match = re.search(r'(\.?\d+\.?\d*)\s*g', name_lower)
    if g_match:
        return float(g_match.group(1))
        
    # Pattern for 'mg'
    mg_match = re.search(r'(\.?\d+\.?\d*)\s*mg', name_lower)
    if mg_match:
        return float(mg_match.group(1)) / 1000.0
        
    return np.nan

def parse_cresco_products(products, store_name):
    """
    Parses the 'data' array from the Cresco API response.
    """
    parsed_products = []
    
    for product in products:
        try:
            data = {
                'Name': product.get('name', 'N/A'),
                'Brand': product.get('brand', 'N/A'),
                'Store': store_name,
                'Type': product.get('category', 'N/A'),
                'Subtype': np.nan, # Not explicitly provided in top level
            }

            # --- Price ---
            price = product.get('discounted_price') or product.get('price')
            data['Price'] = float(price) if price is not None else np.nan

            # --- Weight ---
            # Cresco doesn't have a clean weight field. We extract from the name.
            data['Weight_Str'] = 'N/A'
            data['Weight'] = extract_weight_from_cresco_name(data['Name'])
            
            # --- Cannabinoids ---
            data['THC'] = product.get('bt_potency_thc')
            data['THCa'] = product.get('bt_potency_thca')
            data['CBD'] = product.get('bt_potency_cbd')
            # Add other cannabinoids if available
            data['CBG'] = product.get('bt_potency_cbg')
            data['CBN'] = product.get('bt_potency_cbn')

            # --- Terpenes ---
            terpene_data = {terp: np.nan for terp in KNOWN_TERPENES}
            total_terps = 0
            
            # Terpenes are in a nested 'terpenes' list
            terpenes_list = product.get('terpenes', [])
            if terpenes_list:
                for terp in terpenes_list:
                    name = terp.get('terpene')
                    value = terp.get('value')
                    
                    if name and value is not None:
                        # API uses names like 'beta_myrcene', so we standardize
                        clean_name = name.strip().lower().replace('-', '_')
                        standard_name = TERPENE_MAPPING.get(clean_name)
                        
                        if standard_name:
                            terpene_data[standard_name] = value
                            total_terps += value
            
            # Add bt_potency_terps as Total_Terps if list is empty
            if total_terps == 0 and product.get('bt_potency_terps'):
                total_terps = product.get('bt_potency_terps')

            data.update(terpene_data)
            data['Total_Terps'] = total_terps if total_terps > 0 else np.nan
            
            parsed_products.append(data)

        except Exception as e:
            print(f"Error parsing product: {product.get('name')}. Error: {e}")
            continue
            
    return parsed_products

def fetch_cresco_data():
    """
    Main function to orchestrate the Cresco scraping process.
    """
    all_products_list = []
    
    print("Starting Cresco (Sunnyside) Scraper (api.crescolabs.com)...")

    for store_name, store_id in STORES.items():
        print(f"Fetching data for Sunnyside store: {store_name} (ID: {store_id})...")
        
        # Create a headers copy and set the specific store_id
        headers = HEADERS.copy()
        headers['store_id'] = store_id
        
        for category in CATEGORIES:
            page = 0
            limit = 50
            total_scraped = 0
            while True:
                try:
                    params = {
                        'category': category,
                        'inventory_type': 'retail',
                        'require_sellable_quantity': 'true',
                        'include_specials': 'true',
                        'sellable': 'true',
                        'order_by': 'brand',
                        'limit': str(limit),
                        'usage_type': 'medical',
                        'hob_first': 'true',
                        'include_filters': 'true',
                        'include_facets': 'true',
                        'offset': str(page * limit)
                    }
                    
                    response = requests.get(BASE_URL, headers=headers, params=params, timeout=10)
                    response.raise_for_status()
                    
                    json_response = response.json()
                    products = json_response.get('data')
                    
                    if not products:
                        print(f"  ...completed category: {category}. Found {total_scraped} products.")
                        break
                        
                    parsed_products = parse_cresco_products(products, store_name)
                    all_products_list.extend(parsed_products)
                    total_scraped += len(parsed_products)
                    
                    # Check if this is the last page
                    if len(products) < limit:
                        print(f"  ...completed category: {category}. Found {total_scraped} products.")
                        break
                        
                    page += 1
                    
                except requests.exceptions.RequestException as e:
                    print(f"Error fetching page {page} for {category} at {store_name}: {e}")
                    break
                except Exception as e:
                    print(f"An error occurred processing page {page} for {category}: {e}")
                    break

    if not all_products_list:
        print("No product data was fetched from Cresco. Returning an empty DataFrame.")
        return pd.DataFrame()

    # --- Final DataFrame ---
    df = pd.DataFrame(all_products_list)

    # Calculate DPG
    df['dpg'] = df['Price'] / df['Weight']
    
    # Define column order
    cannabinoid_cols = sorted([col for col in df.columns if col not in KNOWN_TERPENES + ['Name', 'Store', 'Brand', 'Type', 'Subtype', 'Weight', 'Weight_Str', 'Price', 'dpg', 'Total_Terps']])
    terpene_cols = KNOWN_TERPENES

    column_order = (
        ['Name', 'Store', 'Brand', 'Type', 'Subtype', 'Weight', 'Weight_Str', 'Price', 'dpg', 'Total_Terps'] +
        cannabinoid_cols +
        terpene_cols
    )
    
    # Reorder and fill NaNs
    df = df.reindex(columns=column_order).fillna(np.nan)

    print(f"\nScraping complete for Cresco. DataFrame created with {len(df)} rows.")
    return df

