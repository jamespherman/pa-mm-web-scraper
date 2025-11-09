# scrapers/iheartjane_scraper.py
# This scraper is responsible for fetching data from the iHeartJane platform.
# It uses the v2/smart API, which is a more recent and robust endpoint
# compared to their older, deprecated APIs.

import requests
import pandas as pd
import re
from .scraper_utils import convert_to_grams, MASTER_TERPENE_MAP, brand_map, MASTER_CATEGORY_MAP

# --- API Constants ---
# These constants define the endpoint and authentication for the iHeartJane API.
NEW_JANE_URL = "https://dmerch.iheartjane.com/v2/smart"
NEW_JANE_API_KEY = "ce5f15c9-3d09-441d-9bfd-26e87aff5925"


def parse_terpenes_from_text(text):
    """
    Parses terpene data from a raw text block using regular expressions.

    The iHeartJane API often embeds lab data in a free-text field like 'store_notes'
    or 'description'. This function uses a regex pattern to find all occurrences of
    terpene names followed by a percentage value.

    Args:
        text (str): The block of text to parse.

    Returns:
        dict: A dictionary of found terpenes and their values, including 'Total_Terps'.
    """
    terp_data = {}
    if not text:
        return terp_data

    # This regex pattern is designed to capture terpene names (which may include
    # spaces and hyphens) and their corresponding percentage value.
    pattern = r"([a-zA-Z\s-]+)[\s:]*([\d\.]+)%"
    matches = re.findall(pattern, text, re.IGNORECASE)

    total_terps = 0
    for name, value in matches:
        # Standardize the found terpene name using the MASTER_TERPENE_MAP.
        clean_name = name.strip().lower()
        official_name = MASTER_TERPENE_MAP.get(clean_name)

        if official_name:
            val = float(value)
            # This check prevents double-counting if a terpene is listed twice.
            if official_name not in terp_data:
                terp_data[official_name] = val
                total_terps += val

    if total_terps > 0:
        terp_data['Total_Terps'] = round(total_terps, 3)

    return terp_data

# A mapping to standardize cannabinoid names from the API response to our
# desired column names.
CANNABINOID_MAPPING = {
    'thca_potency': 'THCa', 'cbd_potency': 'CBD', 'cbg_potency': 'CBG',
    'cbn_potency': 'CBN', 'thc_potency': 'THC', 'delta_9_thc_potency': 'THC',
    'delta_8_thc_potency': 'Delta-8 THC'
}

def parse_jane_product(product_hit, store_name):
    """
    Parses a single product 'hit' from the iHeartJane API JSON response.

    This function extracts all relevant information for a single product and its
    variants (different weights and prices), returning a list of dictionaries,
    where each dictionary represents a distinct product variant.

    Args:
        product_hit (dict): The JSON object for a single product.
        store_name (str): The name of the store being scraped.

    Returns:
        list: A list of dictionaries, each representing a product variant.
    """
    if 'search_attributes' not in product_hit:
        return []

    product_variants = []
    attrs = product_hit['search_attributes']

    # 1. Extract common data shared across all variants of this product.
    raw_brand = attrs.get('brand')
    raw_category = attrs.get('kind')
    common_data = {
        'Name': attrs.get('name'),
        'Brand': brand_map.get(raw_brand, raw_brand),
        'Type': MASTER_CATEGORY_MAP.get(raw_category.lower(), raw_category) if raw_category else None,
        'Subtype': attrs.get('kind_subtype'),
        'Store': store_name,
        'THC': attrs.get('percent_thc')
    }

    # 2. Extract potency data from the correct nested field.
    inventory_potencies = attrs.get('inventory_potencies', [])
    target_potencies = {}
    for pot in inventory_potencies:
        if pot.get('price_id', '') in ['gram', 'eighth_ounce']:
            target_potencies = pot
            break
    if not target_potencies and inventory_potencies:
        target_potencies = inventory_potencies[0]

    if target_potencies:
        for api_field, our_field in CANNABINOID_MAPPING.items():
            value = target_potencies.get(api_field)
            if value is not None:
                common_data[our_field] = value

    # 3. Extract terpene data by parsing text fields.
    notes = attrs.get('store_notes', '') or attrs.get('description', '')
    terpene_data = parse_terpenes_from_text(notes)
    common_data.update(terpene_data)

    # 4. Process all available price/weight variants.
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
