# scrapers/cresco_scraper.py
# This scraper fetches data from the Cresco/Sunnyside API.

import requests
import pandas as pd
import numpy as np
from .scraper_utils import (
    convert_to_grams, BRAND_MAP, MASTER_CATEGORY_MAP,
    MASTER_SUBCATEGORY_MAP, MASTER_COMPOUND_MAP
)
import re

# --- Constants ---
BASE_URL = "https://api.crescolabs.com/p/inventory/op/fifo-inventory"

# Headers discovered from browser inspection.
# We are *intentionally* omitting the 'authorization' token, as it is
# user-specific and expires quickly. The 'x-api-key' is the stable key.
HEADERS = {
    "accept": "application/json, text/plain, */*", "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-US,en;q=0.9,it-IT;q=0.8,it;q=0.7", "ordering_app_id": "9ha3c289-1260-4he2-nm62-4598bca34naa",
    "origin": "https://www.sunnyside.shop", "referer": "https://www.sunnyside.shop/",
    "sec-ch-ua": "\"Microsoft Edge\";v=\"141\", \"Not?A_Brand\";v=\"8\", \"Chromium\";v=\"141\"",
    "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": "\"Windows\"", "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors", "sec-fetch-site": "cross-site",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0",
    "x-api-key": "hE1gQuwYcO54382jYNH0c9W0w4fEC3dJ8ljnwVau", "x-client-version": "4.20.0"
}

CATEGORIES = ["flower", "vapes", "concentrates"]

def extract_weight_from_cresco_name(name):
    """
    Extracts weight in grams from a product name string (e.g., "3.5g", "1g").
    Uses regex to find patterns like '3.5g', '1g', '500mg'.
    """
    name_lower = name.lower()
    g_match = re.search(r'(\.?\d+\.?\d*)\s*g', name_lower)
    if g_match: return float(g_match.group(1))
    mg_match = re.search(r'(\.?\d+\.?\d*)\s*mg', name_lower)
    if mg_match: return float(mg_match.group(1)) / 1000.0
    return np.nan

def parse_cresco_products(products, store_name):
    """Parses the 'data' array from the Cresco API response."""
    parsed_products = []
    for product in products:
        # Standardize category and skip if not in map
        category_name = product.get('category')
        standardized_category = MASTER_CATEGORY_MAP.get(category_name)
        if not standardized_category:
            continue

        # Standardize brand and subcategory
        brand_name = product.get('brand', 'N/A')
        sub_category_name = product.get('sku', {}).get('product', {}).get('sub_category')

        data = {
            'Name': product.get('name', 'N/A'),
            'Brand': BRAND_MAP.get(brand_name, brand_name),
            'Store': store_name,
            'Type': standardized_category,
            'Subtype': MASTER_SUBCATEGORY_MAP.get(sub_category_name, sub_category_name)
        }

        # Pricing and weight
        price = product.get('discounted_price') or product.get('price')
        data['Price'] = float(price) if price is not None else np.nan
        data['Weight_Str'] = 'N/A'
        data['Weight'] = extract_weight_from_cresco_name(data['Name'])

        # Process compounds
        compounds_dict = {}
        potency_dict = product.get('potency', {})
        if potency_dict:
            for key, value in potency_dict.items():
                standard_name = MASTER_COMPOUND_MAP.get(key)
                if standard_name:
                    compounds_dict[standard_name] = value

        data.update(compounds_dict)
        parsed_products.append(data)

    return parsed_products

def fetch_cresco_data(stores):
    """Main function to orchestrate the Cresco scraping process."""
    all_products_list = []
    print("Starting Cresco (Sunnyside) Scraper (api.crescolabs.com)...")

    for store_name, store_id in stores.items():
        print(f"Fetching data for Sunnyside store: {store_name} (ID: {store_id})...")
        headers = HEADERS.copy()
        headers['store_id'] = store_id
        
        for category in CATEGORIES:
            page, limit, total_scraped = 0, 50, 0
            while True:
                try:
                    params = {'category': category, 'inventory_type': 'retail', 'require_sellable_quantity': 'true',
                              'include_specials': 'true', 'sellable': 'true', 'order_by': 'brand',
                              'limit': str(limit), 'usage_type': 'medical', 'hob_first': 'true',
                              'include_filters': 'true', 'include_facets': 'true', 'offset': str(page * limit)}
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

    df = pd.DataFrame(all_products_list)
    df['dpg'] = df['Price'] / df['Weight']

    print(f"\nScraping complete for Cresco. DataFrame created with {len(df)} rows.")
    return df
