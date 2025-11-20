# scrapers/iheartjane_scraper.py
# -----------------------------------------------------------------------------
# This scraper handles the iHeartJane platform (used by Rise and Maitri/Vytal).
#
# It uses the "Algolia" search API, which is the engine that powers the search
# bar on their websites.
#
# Key Logic:
# 1. It mimics the browser's request to Algolia.
# 2. It handles "pagination" (fetching pages 1, 2, 3...) to get all products.
# 3. It has complex logic to find the "weight" of a product, checking multiple
#    fields (net_weight, quantity_value, product name) in order of reliability.
# -----------------------------------------------------------------------------

import requests # Internet requests.
import re # Regex for text parsing.
import pandas as pd # Data tables.
import numpy as np # Math/NaN.
import json # JSON handling.
import time # Time functions.
from .scraper_utils import (
    convert_to_grams, BRAND_MAP, MASTER_CATEGORY_MAP,
    MASTER_SUBCATEGORY_MAP, MASTER_COMPOUND_MAP, save_raw_json
)

# --- API Constants ---
# The main endpoint for the Algolia search engine.
ALGOLIA_URL = "https://search.iheartjane.com/1/indexes/menu-products-production/query"

# Standard parameters for the Algolia query.
ALGOLIA_QUERY_PARAMS = {
    'x-algolia-agent': 'Algolia for JavaScript (4.20.0); Browser'
}

# --- Platform Configuration ---
# Different dispensary chains (Rise vs Maitri) use different API Keys for Algolia.
# We group them here so we can loop through them easily.
ALGOLIA_PLATFORMS = [
    {
        "platform_name": "Vytal/Maitri",
        "headers": {
            'Accept': '*/*',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Origin': 'https://vytaloptions.com',
            'Referer': 'https://vytaloptions.com/',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15',
            'x-algolia-api-key': 'edc5435c65d771cecbd98bbd488aa8d3',
            'x-algolia-application-id': 'VFM4X0N23A'
        },
        "stores": {
            "Maitri (PGH)": 2913,
            "Maitri (New Stanton)": 4467,
            "Maitri (Uniontown)": 2914,
            "Vytal (Harrisburg)": 6078,
            "Vytal (Fogelsville)": 6079,
            "Vytal (Kennet Square)": 6080,
            "Vytal (Lancaster)": 6081,
            "Vytal (Lansdale)": 6082,
            "Vytal (State College)": 6083,
        }
    },
    {
        "platform_name": "Rise",
        "headers": {
            'Accept': '*/*',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Origin': 'https.risecannabis.com',
            'Referer': 'https.risecannabis.com/',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15',
            'x-algolia-api-key': '11f0fcaee5ae875f14a915b07cb6ef27',
            'x-algolia-application-id': 'VFM4X0N23A'
        },
        "stores": {
            "RISE (Carlisle)": 1547,
            "RISE (Chambersburg)": 1867,
            "RISE (Cranberry)": 1575,
            "RISE (Duncansville)": 1961,
            "RISE (Erie on Lake)": 392,
            "RISE (Erie on Peach)": 2607,
            "RISE (Grove City)": 5202,
            "RISE (Hermitage)": 1551,
            "RISE (King of Prussia)": 1552,
            "RISE (Latrobe)": 1549,
            "RISE (Lebanon)": 6520,
            "RISE (Meadville)": 2863,
            "RISE (Mechanicsburg)": 1550,
            "RISE (Monroeville)": 2266,
            "RISE (New Castle)": 1545,
            "RISE (Philadelphia)": 5383,
            "RISE (Steelton)": 1544,
            "RISE (Warminster)": 3404,
            "RISE (York)": 1548
        }
    }
]


# --- Parsing Functions ---

def _parse_weight_from_name_field(name_str):
    """
    Helper function to extract weight from the product name string.
    Looks for patterns like "[3.5g]" or "[500mg]".

    Args:
        name_str (str): The product name.

    Returns:
        float: Weight in grams, or None.
    """
    if not isinstance(name_str, str):
        return None

    # Regex explanation:
    # \[       : Match a literal opening bracket
    # ([\d\.]+) : Group 1 - Match numbers (digits or dots)
    # \s*      : Match optional whitespace
    # (mg|g)   : Group 2 - Match "mg" or "g"
    # \]       : Match a literal closing bracket
    match = re.search(r'\[([\d\.]+)\s*(mg|g)\]', name_str, re.IGNORECASE)
    
    if not match:
        return None  # No weight found

    try:
        value = float(match.group(1))
        unit = match.group(2).lower()
        
        if unit == 'mg':
            return value / 1000.0  # Convert mg to g
        elif unit == 'g':
            return value
    except Exception:
        return None
    
    return None

def parse_terpenes_from_text(text):
    """
    Parses chemical data from a plain text block (e.g., "Notes" section).
    This is a backup if the structured data is missing.

    Args:
        text (str): The text description to search.

    Returns:
        dict: Found compounds and their values.
    """
    compounds_dict = {}
    if not text:
        return compounds_dict

    # Regex to find "Terpene Name: 1.23%" patterns
    pattern = r"([a-zA-Z\s_-]+)[\s:]*([\d\.]+)%"
    matches = re.findall(pattern, text, re.IGNORECASE)

    for name, value in matches:
        standard_name = MASTER_COMPOUND_MAP.get(name.strip())
        if standard_name:
            compounds_dict[standard_name] = float(value)

    return compounds_dict

def parse_jane_product(product_hit, store_name):
    """
    Parses a single product 'hit' from the JSON response.

    This function is the core logic for interpreting iHeartJane data.
    It handles:
    1. Categorization
    2. Compound extraction (checking multiple sources)
    3. Weight extraction (checking multiple sources with a hierarchy of trust)

    Args:
        product_hit (dict): The raw JSON object for one product.
        store_name (str): The name of the store.

    Returns:
        list: A list of parsed product dictionaries (usually just one, sometimes more).
    """
    
    # 1. Standardize category
    category_name = product_hit.get('kind')
    standardized_category = MASTER_CATEGORY_MAP.get(category_name)
    if not standardized_category:
        return [] # Skip if unknown category

    # 2. Standardize brand and subcategory
    brand_name = (product_hit.get('brand') or 'N/A').strip()
    subcategory_name = product_hit.get('kind_subtype')

    # 3. Define Common Data
    # This data applies to the product regardless of its weight variant.
    common_data = {
        'Name': product_hit.get('name'),
        'Brand': BRAND_MAP.get(brand_name, brand_name),
        'Type': standardized_category,
        'Subtype': MASTER_SUBCATEGORY_MAP.get(subcategory_name, subcategory_name),
        'Store': store_name,
    }

    # 4. Tiered Compound Parsing
    # We look for chemical data in 3 places, in order of preference.
    compounds_found = False

    # A. Try structured lab results (Best)
    lab_results = product_hit.get('lab_results', [])
    if lab_results:
        for result_group in lab_results:
            results_list = result_group.get('lab_results', [])
            if isinstance(results_list, list):
                for result in results_list:
                    compound_name = result.get('compound_name')
                    standard_name = MASTER_COMPOUND_MAP.get(compound_name)
                    if standard_name:
                        common_data[standard_name] = result.get('value')
                        compounds_found = True

    # B. Try parsing the "Store Notes" text (Backup)
    if not compounds_found:
        store_notes = product_hit.get('store_notes', '')
        if store_notes:
            common_data.update(parse_terpenes_from_text(store_notes))
            # Check if we actually found any valid compounds
            compounds_found = any(key in MASTER_COMPOUND_MAP.values() for key in common_data)

    # C. Try "compound_names" list (Last Resort - usually just names without values)
    if not compounds_found:
        for name in product_hit.get('compound_names', []):
            standard_name = MASTER_COMPOUND_MAP.get(name)
            if standard_name:
                common_data[standard_name] = np.nan # We know it exists, but not the amount

    # 5. Weight Parsing Logic
    # We try 3 strategies to find the weight, from most to least reliable.
    product_variants = []
    
    valid_weight = None
    weight_source = None
    
    # Strategy 1: Trust the API's explicit weight fields (Gold Standard)
    net_weight = product_hit.get('net_weight_grams')
    if net_weight and isinstance(net_weight, (int, float)) and net_weight > 0:
        valid_weight = float(net_weight)
        weight_source = "net_weight_grams"
        
    if valid_weight is None:
        q_val = product_hit.get('quantity_value')
        q_unit = product_hit.get('quantity_units')
        if q_val and q_unit == 'g' and isinstance(q_val, (int, float)):
             valid_weight = float(q_val)
             weight_source = "quantity_value"

    # Strategy 2: Regex on Name (Silver Standard)
    # Look for "[3.5g]" in the title.
    if valid_weight is None:
        weight_from_name = _parse_weight_from_name_field(common_data['Name'])
        if weight_from_name:
            valid_weight = weight_from_name
            weight_source = "regex_name"

    # If we found a valid weight, create the product entry.
    if valid_weight:
        price_each = product_hit.get('price_each')
        if price_each:
            price = float(price_each)
            # Check for discounted price
            special_price_data = product_hit.get('special_price_each') or {}
            if special_price_data.get('discount_price'):
                price = float(special_price_data['discount_price'])

            variant_data = common_data.copy()
            variant_data['Price'] = price
            variant_data['Weight'] = valid_weight
            variant_data['Weight_Str'] = f"{valid_weight}g ({weight_source})"
            product_variants.append(variant_data)
            return product_variants

    # Strategy 3: Legacy Fallback (Bronze Standard)
    # Check 'available_weights' list or assume "Each" if nothing else works.
    available_weights = product_hit.get('available_weights', [])
    
    if not available_weights:
        price_each = product_hit.get('price_each')
        if price_each:
            variant_data = common_data.copy()
            special_price_data = product_hit.get('special_price_each') or {}
            discount_price = special_price_data.get('discount_price')

            variant_data['Price'] = float(discount_price or price_each)
            variant_data['Weight_Str'] = "Each"
            variant_data['Weight'] = np.nan
            product_variants.append(variant_data)
            
        return product_variants

    for weight_str in available_weights:
        # Construct field names dynamically (e.g., "price_3.5g")
        price_field = f"price_{weight_str.replace(' ', '_')}"
        special_price_field = f"special_price_{weight_str.replace(' ', '_')}"

        price = product_hit.get(price_field)
        
        special_price_data = product_hit.get(special_price_field, {})
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

# --- Main Fetch Function ---

def _fetch_store_menu(store_id, store_name, headers):
    """
    Fetches all products for a single store.
    Handles pagination (looping through pages).
    """
    print(f"Fetching data for iHeartJane store: {store_name} (ID: {store_id})...")
    all_product_variants = []
    page = 0
    
    while True:
        try:
            # Build the request payload.
            # We ask for everything ("*") filtered by store_id.
            payload = {
                "query": "",
                "filters": f"store_id : {store_id}",
                "facets": ["*"],
                "page": page,
                "hitsPerPage": 1000 # Ask for 1000 items per page to minimize requests
            }
            
            # Send POST request with raw JSON data
            response = requests.post(
                ALGOLIA_URL,
                params=ALGOLIA_QUERY_PARAMS,
                headers=headers,
                data=json.dumps(payload),
                timeout=20
            )
            response.raise_for_status()
            data = response.json()

            # Save raw data for debugging
            filename_parts = ['iheartjane', store_name, f'p{page}']
            save_raw_json(data, filename_parts)
            
            hits = data.get('hits', [])
            if not hits:
                break # No more products
            
            print(f"  ...retrieved {len(hits)} products from page {page}.")

            for hit in hits:
                # Parse each product
                product_variants = parse_jane_product(hit, store_name)
                all_product_variants.extend(product_variants)
            
            page += 1
            
            # Check if we reached the last page
            if page >= data.get('nbPages', 1):
                break

        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for store {store_id} on page {page}: {e}")
            break
        except Exception as e:
            print(f"Error parsing data for store {store_id} on page {page}: {e}")
            print("\n*** ERROR CAUGHT! Interrogating problematic 'hit'... ***")
            break
            
    print(f"Successfully fetched {len(all_product_variants)} product variants for {store_name}.")
    return all_product_variants


def fetch_iheartjane_data():
    """
    Main function to run the iHeartJane scraper.
    Loops through all platforms and stores.
    """
    print("Starting iHeartJane (Algolia) Scraper...")
    all_products_list = []
    
    for platform in ALGOLIA_PLATFORMS:
        print(f"\n--- Scraping Platform: {platform['platform_name']} ---")
        platform_headers = platform['headers']
        
        for store_name, store_id in platform['stores'].items():
            store_variants = _fetch_store_menu(store_id, store_name, platform_headers)
            if store_variants:
                all_products_list.extend(store_variants)
            
    if all_products_list:
        print(f"\nScraping complete for iHeartJane. DataFrame created with {len(all_products_list)} rows.")
        return pd.DataFrame(all_products_list)
    else:
        print("\nNo data fetched from iHeartJane. Returning an empty DataFrame.")
        return pd.DataFrame()
