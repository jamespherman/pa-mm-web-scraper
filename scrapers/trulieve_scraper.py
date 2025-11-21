# scrapers/trulieve_scraper.py
# -----------------------------------------------------------------------------
# This scraper handles the Trulieve dispensary website.
#
# It interacts with Trulieve's "v2" API.
#
# Key features of this API:
# 1. It organizes products by "variants". For example, one "product" might be
#    "Blue Dream Flower", and its variants could be "3.5g", "7g", etc.
#    We need to "flatten" this so each variant is its own row in our data.
# 2. It uses pagination to show results across multiple pages.
# -----------------------------------------------------------------------------

import requests # Used to send internet requests.
import pandas as pd # Used for data tables.
import numpy as np # Used for math.
from .scraper_utils import (
    convert_to_grams, BRAND_MAP, MASTER_CATEGORY_MAP,
    MASTER_SUBCATEGORY_MAP, MASTER_COMPOUND_MAP, save_raw_json
)
import re # Regex for text patterns.

# --- Constants ---
# Updated BASE_URL as per instructions.
BASE_URL = "https://api.trulieve.com/api/v2/menu/{store_id}/all/RECREATIONAL"

# Headers to make us look like a real browser.
HEADERS = {
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.9",
    "origin": "https://www.trulieve.com",
    "referer": "https://www.trulieve.com/",
    "sec-ch-ua": "\"Microsoft Edge\";v=\"123\", \"Not:A-Brand\";v=\"8\", \"Chromium\";v=\"123\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\"",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
}

def parse_trulieve_products(products, store_name):
    """
    Parses the list of products from the Trulieve API.

    Special Logic:
    Trulieve nests different sizes/prices under a 'variants' list for each product.
    We iterate through these variants to create a separate entry for each one.

    Args:
        products (list): Raw product data from API.
        store_name (str): Name of the store.

    Returns:
        list: A flat list of product variants.
    """
    parsed_variants = []
    
    for product in products:
        
        # --- 1. Category Standardization ---
        category_name = product.get('category')
        standardized_category = MASTER_CATEGORY_MAP.get(category_name)

        # Skip if we don't recognize the category.
        if not standardized_category:
            continue

        # --- 2. Brand and Subcategory ---
        brand_name = product.get('brand', 'N/A')
        subcategory_name = product.get('subcategory')

        # --- 3. Build Common Data ---
        # This information applies to ALL variants of this product (e.g. the Name and Brand).
        common_data = {
            'Name': product.get('name', 'N/A'),
            'Brand': BRAND_MAP.get(brand_name, brand_name),
            'Type': standardized_category,
            'Subtype': MASTER_SUBCATEGORY_MAP.get(subcategory_name, subcategory_name),
            'Store': store_name,
            'THC': product.get('thc_content'),
            'CBD': product.get('cbd_content'),
        }

        # --- 4. Parse Terpenes ---
        # Updated logic to handle terpenes located at 'product.get("terpenes")' directly.
        # We assume it is a list of objects with 'name' and 'value'.
        terpenes = product.get('terpenes')
        if terpenes and isinstance(terpenes, list):
            for terpene in terpenes:
                terpene_name = terpene.get('name')
                terpene_value = terpene.get('value')

                # If we have both a name and a value...
                if terpene_name and terpene_value is not None:
                    # ...check if it's in our master map.
                    standard_name = MASTER_COMPOUND_MAP.get(terpene_name)
                    if standard_name:
                        common_data[standard_name] = terpene_value

        # --- 5. Handle Variants ---
        # This is where we split one "product" into multiple rows based on weight/price.
        variants = product.get('variants', [])
        if not variants:
            continue
            
        for variant in variants:
            weight_str = variant.get('option') # e.g., "3.5g"
            if not weight_str:
                continue

            # Get the price (sale price or regular price).
            price = variant.get('sale_unit_price') or variant.get('unit_price')
            if not price:
                continue

            # Create a copy of the common data so we don't mess up other variants.
            product_row = common_data.copy()

            # Update the copy with specific info for this variant.
            product_row.update({
                'Weight': convert_to_grams(weight_str), # Use our helper to get grams
                'Weight_Str': weight_str,
                'Price': float(price),
            })

            parsed_variants.append(product_row)

    return parsed_variants


def fetch_trulieve_data(stores):
    """
    Main function to scrape Trulieve data.
    """
    all_products_list = []
    print("Starting Trulieve Scraper (api.trulieve.com)...")

    for store_name, store_id in stores.items():
        print(f"Fetching data for Trulieve store: {store_name} (ID: {store_id})...")

        page = 1
        while True:
            try:
                # Build the URL for this specific page.
                url = f"{BASE_URL.format(store_id=store_id)}?page={page}"

                # Send request
                response = requests.get(url, headers=HEADERS, timeout=10)
                response.raise_for_status()
                json_response = response.json()

                # --- Save Raw Data ---
                filename_parts = ['trulieve', store_name, 'all', f'p{page}']
                save_raw_json(json_response, filename_parts)

                # Get products
                products = json_response.get('data')

                # Stop if no products found.
                if not products:
                    print(f"  ...no products found on page {page}. Stopping.")
                    break
                    
                # Parse and add to list
                all_products_list.extend(parse_trulieve_products(products, store_name))

                # Check pagination info to see if we are on the last page.
                last_page = json_response.get('last_page')
                current_page = json_response.get('current_page')

                # Check meta if not at root
                if last_page is None:
                    meta = json_response.get('meta', {})
                    last_page = meta.get('last_page')
                    current_page = meta.get('current_page')

                if last_page is not None and current_page is not None and current_page >= last_page:
                    print(f"  ...reached last page ({last_page}).")
                    break

                page += 1

            except requests.exceptions.RequestException as e:
                print(f"Error fetching page {page} for {store_name}: {e}")
                break
            except Exception as e:
                print(f"An error occurred processing page {page}: {e}")
                break

    if not all_products_list:
        print("No product data was fetched from Trulieve. Returning an empty DataFrame.")
        return pd.DataFrame()

    df = pd.DataFrame(all_products_list)

    # Calculate Dollars Per Gram
    if not df.empty and 'Price' in df.columns and 'Weight' in df.columns:
        df['dpg'] = df['Price'] / df['Weight']

    print(f"\nScraping complete for Trulieve. DataFrame created with {len(df)} rows.")
    return df
