# scrapers/iheartjane_scraper.py
# This scraper is responsible for fetching data from the iHeartJane Algolia API.
# It has been refactored to replace the old, non-functional 'dmerch' endpoint.

import requests
import re
import pdb
import pandas as pd
import numpy as np
import json
import time
from .scraper_utils import (
    convert_to_grams, BRAND_MAP, MASTER_CATEGORY_MAP,
    MASTER_SUBCATEGORY_MAP, MASTER_COMPOUND_MAP, save_raw_json
)

# --- API Constants ---
# This is the Algolia search endpoint
ALGOLIA_URL = "https://search.iheartjane.com/1/indexes/menu-products-production/query"

# This is the base query param string
ALGOLIA_QUERY_PARAMS = {
    'x-algolia-agent': 'Algolia for JavaScript (4.20.0); Browser'
}

# This is the new "brain" of the scraper.
# It stores the unique headers (API key, Origin) for each platform
# and the list of stores associated with that platform.
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
    # We can add Curaleaf, Ayr, etc. here as new platforms
]


# --- Parsing Functions ---

# --- WEIGHT FROM NAME HELPER FUNCTION ---
def _parse_weight_from_name_field(name_str):
    """
    Parses a weight string (e.g., [7g], [500mg]) from a product name.
    Returns the weight in grams if found, otherwise None.
    """
    if not isinstance(name_str, str):
        return None

    # Regex to find weights in brackets (e.g., [4000mg], [7g], [0.5g])
    # Group 1: The number (e.g., "4000")
    # Group 2: The unit (e.g., "mg" or "g")
    match = re.search(r'\[([\d\.]+)\s*(mg|g)\]', name_str, re.IGNORECASE)
    
    if not match:
        return None  # No weight found in name

    try:
        value = float(match.group(1))
        unit = match.group(2).lower()
        
        if unit == 'mg':
            return value / 1000.0  # Convert mg to g
        elif unit == 'g':
            return value
    except Exception:
        return None # Failed to parse
    
    return None

def parse_terpenes_from_text(text):
    """
    Parses compound data from a raw text block using regular expressions.
    This is a fallback for when structured lab data is not available.
    """
    compounds_dict = {}
    if not text:
        return compounds_dict

    # Regex to find patterns like "Terpene Name: 1.23%"
    pattern = r"([a-zA-Z\s_-]+)[\s:]*([\d\.]+)%"
    matches = re.findall(pattern, text, re.IGNORECASE)

    for name, value in matches:
        standard_name = MASTER_COMPOUND_MAP.get(name.strip())
        if standard_name:
            compounds_dict[standard_name] = float(value)

    return compounds_dict

def parse_jane_product(product_hit, store_name):
    """
    Parses a single product 'hit' from the iHeartJane Algolia API JSON response.
    """
    
    # 1. Standardize category and skip if not in map
    category_name = product_hit.get('kind')
    standardized_category = MASTER_CATEGORY_MAP.get(category_name)
    if not standardized_category:
        return []

    # 2. Standardize brand and subcategory
    brand_name = (product_hit.get('brand') or 'N/A').strip()
    subcategory_name = product_hit.get('kind_subtype')

    # 3. DEFINE COMMON_DATA FIRST (Crucial Fix)
    common_data = {
        'Name': product_hit.get('name'),
        'Brand': BRAND_MAP.get(brand_name, brand_name),
        'Type': standardized_category,
        'Subtype': MASTER_SUBCATEGORY_MAP.get(subcategory_name, subcategory_name),
        'Store': store_name,
    }

    # 4. Tiered Compound Parsing
    compounds_found = False
    # ... Attempt to parse structured lab_results
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

    # ... Fallback to unstructured text parsing
    if not compounds_found:
        store_notes = product_hit.get('store_notes', '')
        if store_notes:
            common_data.update(parse_terpenes_from_text(store_notes))
            compounds_found = any(key in MASTER_COMPOUND_MAP.values() for key in common_data)

    # ... Second fallback to compound_names list
    if not compounds_found:
        for name in product_hit.get('compound_names', []):
            standard_name = MASTER_COMPOUND_MAP.get(name)
            if standard_name:
                common_data[standard_name] = np.nan

    # 5. START NEW WEIGHT PARSING LOGIC (Now safe to run)
    product_variants = []
    
    # Strategy 1: Trust the API's explicit weight fields (Gold Standard)
    valid_weight = None
    weight_source = None
    
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
    if valid_weight is None:
        # Now this works because common_data is defined above!
        weight_from_name = _parse_weight_from_name_field(common_data['Name'])
        if weight_from_name:
            valid_weight = weight_from_name
            weight_source = "regex_name"

    # Construct Variant from Valid Weight
    # Extra comment line
    if valid_weight:
        price_each = product_hit.get('price_each')
        if price_each:
            price = float(price_each)
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
    Fetches all product variants for a single iHeartJane store
    using the correct platform-specific headers.
    """
    print(f"Fetching data for iHeartJane store: {store_name} (ID: {store_id})...")
    all_product_variants = []
    page = 0
    
    while True:
        try:
            # Build the simple payload. This is a single request, not a batch.
            # We send this as a raw JSON string.
            payload = {
                "query": "",
                "filters": f"store_id : {store_id}",
                "facets": ["*"],
                "page": page,
                "hitsPerPage": 1000 # Fetch 1000 per page
            }
            
            response = requests.post(
                ALGOLIA_URL,
                params=ALGOLIA_QUERY_PARAMS,
                headers=headers,
                data=json.dumps(payload), # Send as raw JSON string
                timeout=20
            )
            response.raise_for_status()
            data = response.json()

            # Save the raw JSON data
            filename_parts = ['iheartjane', store_name, f'p{page}']
            save_raw_json(data, filename_parts)
            hits = data.get('hits', [])
            
            if not hits:
                # No more products, we are done paginating
                break
            
            print(f"  ...retrieved {len(hits)} products from page {page}.")

            for hit in hits:
                
                # The "hit" is the raw product, which is what our parser expects
                product_variants = parse_jane_product(hit, store_name)
                all_product_variants.extend(product_variants)
            
            page += 1
            
            # Check if we are on the last page
            if page >= data.get('nbPages', 1):
                break

        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for store {store_id} on page {page}: {e}")
            break # Stop scraping this store on an error
        except Exception as e:
            print(f"Error parsing data for store {store_id} on page {page}: {e}")
            
            # --- PDB TRACE ---
            print("\n*** ERROR CAUGHT! Interrogating problematic 'hit'... ***")
            # 'hit' is the variable from the for-loop that caused the error.
            # We are now paused inside the exception handler.
            pdb.set_trace()
            # --- END TRACE ---
                
            break
            
    print(f"Successfully fetched {len(all_product_variants)} product variants for {store_name}.")
    return all_product_variants


def fetch_iheartjane_data():
    """
    Fetches all product data by looping through each platform (Rise, Vytal)
    and scraping all stores associated with it.
    """
    print("Starting iHeartJane (Algolia) Scraper...")
    all_products_list = []
    
    # Loop through each platform (e.g., Rise, Vytal)
    for platform in ALGOLIA_PLATFORMS:
        print(f"\n--- Scraping Platform: {platform['platform_name']} ---")
        platform_headers = platform['headers']
        
        # Loop through all stores for that platform
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
