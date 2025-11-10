# scrapers/trulieve_scraper.py
# This scraper fetches data from the new Trulieve v2 API.

import requests
import pandas as pd
import numpy as np
from .scraper_utils import (
    convert_to_grams, BRAND_MAP, MASTER_CATEGORY_MAP,
    MASTER_SUBCATEGORY_MAP, MASTER_COMPOUND_MAP
)
import re

# --- Constants ---
BASE_URL = "https://api.trulieve.com/api/v2/menu/{store_id}/{category}/MEDICAL"
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

CATEGORIES = ["flower", "vaporizers", "concentrates", "tinctures", "edibles"]

def parse_trulieve_products(products, store_name):
    """
    Parses the 'data' array from the Trulieve API response.
    Each variant of a product becomes a separate row.
    """
    parsed_variants = []
    
    for product in products:
        # Standardize category and skip if not in map
        category_name = product.get('category')
        standardized_category = MASTER_CATEGORY_MAP.get(category_name)
        if not standardized_category:
            continue

        # Standardize brand and subcategory
        brand_name = product.get('brand', 'N/A')
        subcategory_name = product.get('subcategory')

        # Base data for all variants of this product
        common_data = {
            'Name': product.get('name', 'N/A'),
            'Brand': BRAND_MAP.get(brand_name, brand_name),
            'Type': standardized_category,
            'Subtype': MASTER_SUBCATEGORY_MAP.get(subcategory_name, subcategory_name),
            'Store': store_name,
            'THC': product.get('thc_content'),
            'CBD': product.get('cbd_content'),
        }

        # Parse terpenes
        for terpene in product.get('terpenes', []):
            terpene_name = terpene.get('name')
            terpene_value = terpene.get('value')
            if terpene_name and terpene_value is not None:
                standard_name = MASTER_COMPOUND_MAP.get(terpene_name)
                if standard_name:
                    common_data[standard_name] = terpene_value

        # Variants represent different weights/prices
        variants = product.get('variants', [])
        if not variants:
            continue
            
        for variant in variants:
            weight_str = variant.get('option')
            if not weight_str:
                continue

            price = variant.get('sale_unit_price') or variant.get('unit_price')
            if not price:
                continue

            product_row = common_data.copy()
            product_row.update({
                'Weight': convert_to_grams(weight_str),
                'Weight_Str': weight_str,
                'Price': float(price),
            })
            parsed_variants.append(product_row)

    return parsed_variants


def fetch_trulieve_data(stores):
    """
    Main function to orchestrate the Trulieve scraping process.
    """
    all_products_list = []
    print("Starting Trulieve Scraper (api.trulieve.com)...")

    for store_name, store_id in stores.items():
        print(f"Fetching data for Trulieve store: {store_name} (ID: {store_id})...")
        for category in CATEGORIES:
            page = 1
            while True:
                try:
                    url = f"{BASE_URL.format(store_id=store_id, category=category)}?page={page}"
                    response = requests.get(url, headers=HEADERS, timeout=10)
                    response.raise_for_status()
                    
                    json_response = response.json()
                    products = json_response.get('data')
                    
                    if not products:
                        print(f"  ...completed category: {category}")
                        break
                        
                    all_products_list.extend(parse_trulieve_products(products, store_name))
                    
                    last_page, current_page = json_response.get('last_page'), json_response.get('current_page')
                    if last_page is not None and current_page is not None and current_page >= last_page:
                        print(f"  ...completed category: {category}")
                        break
                        
                    page += 1
                    
                except requests.exceptions.RequestException as e:
                    print(f"Error fetching page {page} for {category} at {store_name}: {e}")
                    break
                except Exception as e:
                    print(f"An error occurred processing page {page} for {category}: {e}")
                    break

    if not all_products_list:
        print("No product data was fetched from Trulieve. Returning an empty DataFrame.")
        return pd.DataFrame()

    df = pd.DataFrame(all_products_list)
    df['dpg'] = df['Price'] / df['Weight']

    print(f"\nScraping complete for Trulieve. DataFrame created with {len(df)} rows.")
    return df
