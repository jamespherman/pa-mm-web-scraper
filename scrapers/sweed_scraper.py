# scrapers/sweed_scraper.py
# This scraper fetches data from the Sweed platform (used by Zen Leaf).
# This version is refactored to be efficient:
# 1. Gathers all variant IDs from all stores.
# 2. De-duplicates the list of variant IDs.
# 3. Fetches detailed lab/price data only ONCE for each unique variant.

import requests
import pandas as pd
import numpy as np
import json
import time
from .scraper_utils import (
    convert_to_grams, BRAND_MAP, MASTER_CATEGORY_MAP,
    MASTER_SUBCATEGORY_MAP, MASTER_COMPOUND_MAP
)

# --- Constants ---

URL_PRODUCT_LIST = "https://web-ui-production.sweedpos.com/_api/proxy/Products/GetProductList"
URL_LAB_DATA = "https://web-ui-production.sweedpos.com/_api/proxy/Products/GetExtendedLabdata"
URL_VARIANT_DETAIL = "https://web-ui-production.sweedpos.com/_api/proxy/Products/GetProductByVariantId"

BASE_HEADERS = {
    "Accept": "*/*",
    "Content-Type": "application/json",
    "Origin": "https://zenleafdispensaries.com",
    "Referer": "https://zenleafdispensaries.com/",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15",
}

CATEGORY_MAP = {
    "Flower": 140929,
    "Vaporizers": 140932,
    "Concentrates": 140928, # Their "Extracts"
    "Edibles": 384416,      # Their "Troches"
    "Tinctures": 140930
}

SWED_STORES_TO_SCRAPE = {
    "Zen Leaf (West York - York)": 106,
    "Zen Leaf (Cranberry Twp - Cranberry)": 145,
    "Zen Leaf (Monroeville - Monroeville)": 146,
    "Zen Leaf (West Chester - West Chester)": 147,
    "Zen Leaf (Malvern - Malvern)": 148,
    "Zen Leaf (Pittsburgh - Pittsburgh - Robinson)": 149,
    "Zen Leaf (Harrisburg - Harrisburg)": 150,
    "Zen Leaf (Washington - Washington)": 151,
    "Zen Leaf (Philadelphia - Philadelphia)": 152,
    "Zen Leaf (Sellersville - Sellersville)": 153,
    "Zen Leaf (Altoona - Altoona)": 154,
    "Zen Leaf (Pittsburgh - Pittsburgh - McKnight)": 155,
    "Zen Leaf (Wynnewood - Wynnewood)": 156,
    "Zen Leaf (New Kensington - New Kensington)": 157,
    "Zen Leaf (Abington - Abington)": 158,
    "Zen Leaf (Fairless Hills - Fairless Hills)": 159,
    "Zen Leaf (Clifton Heights - Clifton Heights)": 160,
    "Zen Leaf (Norristown - Norristown)": 233,
}

# --- Main Scraper Functions ---

def fetch_sweed_data():
    """
    Main orchestration function for scraping Sweed (Zen Leaf) stores.
    """
    print("Starting Sweed (Zen Leaf) Scraper (web-ui-production.sweedpos.com)...")

    # --- Step 1: Get the "master list" of all product variants ---
    print("Step 1/3: Fetching all product variants from all stores...")
    master_variant_list = _get_all_variant_info()
    if not master_variant_list:
        print("No product data was fetched from Sweed. Returning an empty DataFrame.")
        return pd.DataFrame()
    print(f"  ...found {len(master_variant_list)} total variant listings.")

    # --- Step 2: Get unique variant IDs and fetch their details ---
    print("Step 2/3: De-duplicating and fetching unique product details...")
    unique_variant_ids = set(v['variant_id'] for v in master_variant_list)
    print(f"  ...found {len(unique_variant_ids)} unique variants to fetch.")
    
    # This returns a dictionary: {variant_id: {details}}
    detailed_data_map = _get_unique_details(unique_variant_ids)
    print(f"  ...successfully fetched details for {len(detailed_data_map)} variants.")

    # --- Step 3: Combine the master list with the detailed data ---
    print("Step 3/3: Combining and standardizing all data...")
    final_product_list = []
    for variant in master_variant_list:
        variant_id = variant['variant_id']
        # Check if we have lab/price data for this variant
        if variant_id in detailed_data_map:
            # Combine the base info (Store, Name, Brand) with the detailed info
            combined_data = {**variant, **detailed_data_map[variant_id]}
            final_product_list.append(combined_data)

    if not final_product_list:
        print("No final product data could be combined. Returning an empty DataFrame.")
        return pd.DataFrame()

    df = pd.DataFrame(final_product_list)
    df['dpg'] = df['Price'] / df['Weight']

    print(f"\nScraping complete for Sweed. DataFrame created with {len(df)} rows.")
    return df


def _get_all_variant_info():
    """
    Step 1: Loops through all stores and categories just to get the
    basic variant info (variant_id, Name, Brand, Store, etc.).
    """
    product_variants = []
    
    for store_name, store_id in SWED_STORES_TO_SCRAPE.items():
        print(f"  - Scanning store: {store_name} (ID: {store_id})...")
        headers = BASE_HEADERS.copy()
        headers["StoreId"] = str(store_id)

        for category_name, category_id in CATEGORY_MAP.items():
            page = 1
            while True:
                payload = {
                    "filters": {"category": [category_id]},
                    "page": page, "pageSize": 100, "sortingMethodId": 7,
                    "searchTerm": "", "platformOs": "web", "sourcePage": 0
                }
                
                try:
                    response = requests.post(URL_PRODUCT_LIST, headers=headers, json=payload, timeout=10)
                    response.raise_for_status()
                    data = response.json()
                    
                    products = data.get('list')
                    if not products:
                        break # No more products on this page, end pagination

                    # Parse the product list
                    for product in products:
                        # FIX: Check for None items in the product list (for 'Edibles' bug)
                        if not product:
                            continue

                        # FIX: Check for None on .get('brand') and .get('subcategory')
                        brand_obj = product.get('brand')
                        brand_name = brand_obj.get('name', 'N/A').strip() if brand_obj else 'N/A'
                        
                        subcategory_obj = product.get('subcategory')
                        subcategory_name = subcategory_obj.get('name') if subcategory_obj else None

                        for variant in product.get('variants', []):
                            data_dict = {
                                "variant_id": variant.get('id'),
                                "Name": product.get('name', 'N/A'),
                                "Brand": BRAND_MAP.get(brand_name, brand_name),
                                "Type": category_name,
                                "Subtype": MASTER_SUBCATEGORY_MAP.get(subcategory_name),
                                "Store": store_name,
                            }
                            product_variants.append(data_dict)
                            
                    page += 1
                    time.sleep(0.2)
                    
                except requests.exceptions.RequestException as e:
                    print(f"    Error fetching {category_name} (Page {page}): {e}")
                    break
                except Exception as e:
                    print(f"    Error parsing {category_name} (Page {page}): {e}")
                    break
                    
    return product_variants


def _get_unique_details(unique_variant_ids):
    """
    Step 2: Loops *only* over the unique variant IDs and fetches
    their price, weight, and lab data.
    """
    # We use a dummy StoreId in the header. As you proved, it doesn't matter.
    headers = BASE_HEADERS.copy()
    headers["StoreId"] = "155" # Use McKnight as our default
    
    detailed_data_map = {}
    
    for i, variant_id in enumerate(unique_variant_ids):
        if (i + 1) % 50 == 0:
            print(f"    ...fetching details for variant {i + 1}/{len(unique_variant_ids)}")
            
        if not variant_id:
            continue
            
        try:
            # This will hold all the details for this one variant
            details = {}

            # --- Call 1: Get Price and Weight ---
            payload_variant = {"variantId": variant_id, "platformOs": "web", "stockType": "Default"}
            resp_variant = requests.post(URL_VARIANT_DETAIL, headers=headers, json=payload_variant, timeout=10)
            resp_variant.raise_for_status()
            variant_data = resp_variant.json()
            
            variant_detail = variant_data.get('variants', [{}])[0]
            
            details['Price'] = variant_detail.get('promoPrice') or variant_detail.get('price')
            details['Weight_Str'] = variant_detail.get('name', 'N/A')
            
            # Parse Weight
            unit_size = variant_detail.get('unitSize', {})
            value = unit_size.get('value')
            unit = unit_size.get('unitAbbr', '').upper()
            if unit == 'G':
                details['Weight'] = float(value)
            elif unit == 'MG':
                details['Weight'] = float(value) / 1000.0
            else:
                details['Weight'] = np.nan

            # --- Call 2: Get Lab Data ---
            payload_lab = {"variantId": variant_id}
            resp_lab = requests.post(URL_LAB_DATA, headers=headers, json=payload_lab, timeout=10)
            resp_lab.raise_for_status()
            lab_data = resp_lab.json()

            # Parse Cannabinoids
            for block in [lab_data.get('thc'), lab_data.get('cbd')]:
                if block and isinstance(block.get('values'), list):
                    for item in block.get('values'):
                        standard_name = MASTER_COMPOUND_MAP.get(item.get('code'))
                        if standard_name:
                            details[standard_name] = item.get('min')

            # Parse Terpenes
            terp_block = lab_data.get('terpenes')
            if terp_block and isinstance(terp_block.get('values'), list):
                for item in terp_block.get('values'):
                    code = item.get('code')
                    standard_name = MASTER_COMPOUND_MAP.get(code)
                    if standard_name:
                        details[standard_name] = item.get('min')
            
            # If we got here, it's a success
            detailed_data_map[variant_id] = details
            
            time.sleep(0.1) # Politeness delay
            
        except requests.exceptions.RequestException:
            continue
        except Exception:
            continue
            
    return detailed_data_map
