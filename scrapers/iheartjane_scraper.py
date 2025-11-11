# scrapers/iheartjane_scraper.py
# This scraper is responsible for fetching data from the iHeartJane platform.
# It uses the v2/smart API, which is a more recent and robust endpoint
# compared to their older, deprecated APIs.

import requests
import pandas as pd
import re
from .scraper_utils import (
    convert_to_grams, BRAND_MAP, MASTER_CATEGORY_MAP,
    MASTER_SUBCATEGORY_MAP, MASTER_COMPOUND_MAP
)

# --- API Constants ---
# These constants define the endpoint and authentication for the iHeartJane API.
NEW_JANE_URL = "https://dmerch.iheartjane.com/v2/smart"
NEW_JANE_API_KEY = "ce5f15c9-3d09-441d-9bfd-26e87aff5925"


def parse_terpenes_from_text(text):
    """
    Parses compound data from a raw text block using regular expressions.
    This is a fallback for when structured lab data is not available.
    """
    compounds_dict = {}
    if not text:
        return compounds_dict

    pattern = r"([a-zA-Z\s_-]+)[\s:]*([\d\.]+)%"
    matches = re.findall(pattern, text, re.IGNORECASE)

    for name, value in matches:
        standard_name = MASTER_COMPOUND_MAP.get(name.strip())
        if standard_name:
            compounds_dict[standard_name] = float(value)

    return compounds_dict

def parse_jane_product(product_hit, store_name):
    """
    Parses a single product 'hit' from the iHeartJane API JSON response.
    """
    if 'search_attributes' not in product_hit:
        return []

    attrs = product_hit['search_attributes']

    # Standardize category and skip if not in map
    category_name = attrs.get('kind')
    standardized_category = MASTER_CATEGORY_MAP.get(category_name)
    if not standardized_category:
        return []

    # Standardize brand and subcategory
    brand_name = attrs.get('brand').strip()
    subcategory_name = attrs.get('kind_subtype')

    common_data = {
        'Name': attrs.get('name'),
        'Brand': BRAND_MAP.get(brand_name, brand_name),
        'Type': standardized_category,
        'Subtype': MASTER_SUBCATEGORY_MAP.get(subcategory_name, subcategory_name),
        'Store': store_name,
    }

    # --- Tiered Compound Parsing ---
    compounds_found = False

    # 1. Attempt to parse structured lab_results
    lab_results = attrs.get('lab_results', [])
    if lab_results:
        for result in lab_results:
            compound_name = result.get('compound_name')
            standard_name = MASTER_COMPOUND_MAP.get(compound_name)
            if standard_name:
                common_data[standard_name] = result.get('value')
                compounds_found = True

    # 2. Fallback to unstructured text parsing
    if not compounds_found:
        store_notes = attrs.get('store_notes', '')
        if store_notes:
            common_data.update(parse_terpenes_from_text(store_notes))
            compounds_found = any(key in MASTER_COMPOUND_MAP.values() for key in common_data)

    # 3. Second fallback to compound_names list
    if not compounds_found:
        for name in attrs.get('compound_names', []):
            standard_name = MASTER_COMPOUND_MAP.get(name)
            if standard_name:
                # Value is not available in this field, so mark as present
                common_data[standard_name] = None

    # Process all available price/weight variants
    product_variants = []
    available_weights = attrs.get('available_weights', [])
    if not available_weights:
        if attrs.get('price_each'):
            variant_data = common_data.copy()
            variant_data['Price'] = float(attrs.get('special_price_each', {}).get('discount_price') or attrs.get('price_each'))
            variant_data['Weight_Str'] = "Each"
            variant_data['Weight'] = None
            product_variants.append(variant_data)
        return product_variants

    for weight_str in available_weights:
        price_field = f"price_{weight_str.replace(' ', '_')}"
        special_price_field = f"special_price_{weight_str.replace(' ', '_')}"
        price = attrs.get(price_field)
        
        special_price_data = attrs.get(special_price_field, {})
        if special_price_data and special_price_data.get('discount_price'):
            price = float(special_price_data['discount_price'])

        if not price:
            continue

        variant_data = common_data.copy()
        variant_data['Price'] = float(price)
        variant_data['Weight_Str'] = weight_str
        variant_data['Weight'] = convert_to_grams(weight_str)
        product_variants.append(variant_data)
            
    return product_variants

def fetch_iheartjane_data(store_id, store_name):
    """
    Fetches all product data for a specific iHeartJane store ID.

    This function constructs and sends a POST request to the iHeartJane v2/smart API.
    It mimics the payload of a real browser request to retrieve all products in a
    single call, then parses the response into a pandas DataFrame.

    Args:
        store_id (int): The unique identifier for the iHeartJane store.
        store_name (str): The user-friendly name of the store.

    Returns:
        pd.DataFrame: A DataFrame containing all product variants from the store.
    """
    print(f"Fetching data for iHeartJane store: {store_name} (ID: {store_id})...")
    
    all_products = []
    
    params = { 'jdm_api_key': NEW_JANE_API_KEY, 'jdm_source': 'monolith', 'jdm_version': '2.12.0' }
    
    full_search_facets = [
        "activities", "aggregate_rating", "applicable_special_ids", "available_weights",
        "brand_subtype", "brand", "bucket_price", "category", "feelings",
        "has_brand_discount", "kind", "percent_cbd", "percent_thc", "root_types", "compound_names"
    ]

    payload = {
        "app_mode": "embedded", "jane_device_id": "me7dtQx8hW9YlcYmnHPys", "search_attributes": ["*"],
        "store_id": store_id, "disable_ads": False, "num_columns": 1, "page_size": 60, "page": 0,
        "placement": "menu_inline_table", "search_facets": full_search_facets,
        "search_filter": f"store_id = {store_id}", "search_query": "", "search_sort": "recommendation"
    }

    try:
        response = requests.post(NEW_JANE_URL, params=params, json=payload)
        response.raise_for_status()
        
        data = response.json()
        hits = data.get('products', [])

        print(f"  ...retrieved {len(hits)} products in a single call.")

        for hit in hits:
            product_variants = parse_jane_product(hit, store_name)
            all_products.extend(product_variants)
            
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for store {store_id}: {e}")
            
    if all_products:
        print(f"Successfully fetched {len(all_products)} product variants for {store_name}.")
        return pd.DataFrame(all_products)
    else:
        print(f"No data fetched for {store_name}.")
        return pd.DataFrame()
