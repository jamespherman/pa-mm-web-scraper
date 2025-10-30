# scrapers/iheartjane_scraper.py
# This is the iHeartJane scraper, rebuilt with the new API.

import requests
import pandas as pd
import re # Import the regular expression library
from .scraper_utils import convert_to_grams # Import our new util function

# --- New API Constants ---
NEW_JANE_URL = "https://dmerch.iheartjane.com/v2/smart"
NEW_JANE_API_KEY = "ce5f15c9-3d09-441d-9bfd-26e87aff5925"

# Define the terpenes we want to extract
TERPENE_LIST = [
    'beta-Myrcene', 'Limonene', 'beta-Caryophyllene', 'Terpinolene',
    'Linalool', 'alpha-Pinene', 'beta-Pinene', 'Caryophyllene Oxide',
    'Guaiol', 'Humulene', 'alpha-Bisabolol', 'Camphene', 'Ocimene'
]

# Map all possible name variations to the official terpene name
TERPENE_MAPPING = {
    # beta-Myrcene
    'beta-myrcene': 'beta-Myrcene',
    'myrcene': 'beta-Myrcene',
    'b-myrcene': 'beta-Myrcene',
    # Limonene
    'limonene': 'Limonene',
    'd-limonene': 'Limonene',
    # beta-Caryophyllene
    'beta-caryophyllene': 'beta-Caryophyllene',
    'caryophyllene': 'beta-Caryophyllene',
    'b-caryophyllene': 'beta-Caryophyllene',
    # Terpinolene
    'terpinolene': 'Terpinolene',
    # Linalool
    'linalool': 'Linalool',
    # alpha-Pinene
    'alpha-pinene': 'alpha-Pinene',
    'a-pinene': 'alpha-Pinene',
    # beta-Pinene
    'beta-pinene': 'beta-Pinene',
    'b-pinene': 'beta-Pinene',
    # Caryophyllene Oxide
    'caryophyllene oxide': 'Caryophyllene Oxide',
    # Guaiol
    'guaiol': 'Guaiol',
    # Humulene
    'humulene': 'Humulene',
    'alpha-humulene': 'Humulene',
    'a-humulene': 'Humulene',
    # alpha-Bisabolol
    'alpha-bisabolol': 'alpha-Bisabolol',
    'bisabolol': 'alpha-Bisabolol',
    'a-bisabolol': 'alpha-Bisabolol',
    # Camphene
    'camphene': 'Camphene',
    # Ocimene
    'ocimene': 'Ocimene',
    'beta-ocimene': 'Ocimene',
    'b-ocimene': 'Ocimene'
}

def parse_terpenes_from_text(text):
    """
    Uses regular expressions (regex) to find all terpenes in
    a block of text (like the 'store_notes' field).
    """
    terp_data = {}
    if not text:
        return terp_data

    # This pattern now captures names with spaces and hyphens.
    # e.g., "beta-Myrcene", "beta Myrcene", "b Myrcene"
    pattern = r"([a-zA-Z\s-]+)[\s:]*([\d\.]+)%"

    matches = re.findall(pattern, text, re.IGNORECASE)

    total_terps = 0
    for name, value in matches:
        # Clean up the name and look it up in our mapping
        clean_name = name.strip().lower()
        official_name = TERPENE_MAPPING.get(clean_name)

        if official_name:
            val = float(value)
            # Avoid double-counting if a name is matched twice
            if official_name not in terp_data:
                terp_data[official_name] = val
                total_terps += val

    if total_terps > 0:
        # Round to 3 decimal places
        terp_data['Total_Terps'] = round(total_terps, 3)

    return terp_data

# --- Cannabinoid Definitions ---

CANNABINOID_MAPPING = {
    'thca_potency': 'THCa',
    'cbd_potency': 'CBD',
    'cbg_potency': 'CBG',
    'cbn_potency': 'CBN',
    'thc_potency': 'THC',
    'delta_9_thc_potency': 'THC', # Map Delta-9 to THC
    'delta_8_thc_potency': 'Delta-8 THC'
}

def parse_jane_product(product_hit, store_name):
    """
    Parses a single product from the new "smart" API response.
    """
    if 'search_attributes' not in product_hit:
        return []

    product_variants = []
    
    # All the good data is in 'search_attributes'
    attrs = product_hit['search_attributes']

    # 1. Get common data
    common_data = {
        'Name': attrs.get('name'),
        'Brand': attrs.get('brand'),
        'Type': attrs.get('kind'),
        'Subtype': attrs.get('kind_subtype'),
        'Store': store_name,
        'THC': attrs.get('percent_thc'), # Default value
    }

    # 1a. Get Potency Data from the Correct Field
    # The data is in 'inventory_potencies', not the top level!
    inventory_potencies = attrs.get('inventory_potencies', [])

    # We prefer the 'gram' or 'eighth_ounce' potency data if available
    target_potencies = {}
    for pot in inventory_potencies:
        # The 'price_id' is 'gram', 'eighth_ounce', 'half_ounce', etc.
        price_id = pot.get('price_id', '')
        if price_id in ['gram', 'eighth_ounce']:
            target_potencies = pot
            break # Found our preferred one, stop looking

    # If we didn't find a preferred one, use the first available
    if not target_potencies and inventory_potencies:
        target_potencies = inventory_potencies[0]

    # Now, extract all cannabinoids using our mapping
    if target_potencies:
        for api_field, our_field in CANNABINOID_MAPPING.items():
            value = target_potencies.get(api_field)
            if value is not None:
                common_data[our_field] = value

    # 2. Get lab data (terpenes)
    # We'll parse the 'store_notes' or 'description' field
    notes = attrs.get('store_notes', '')
    if not notes:
        notes = attrs.get('description', '')
        
    terpene_data = parse_terpenes_from_text(notes)
    common_data.update(terpene_data) # Add all found terpenes

    # 3. Process price/weight variants
    available_weights = attrs.get('available_weights', [])
    if not available_weights:
        # Fallback for "each" items
        if attrs.get('price_each'):
            variant_data = common_data.copy()
            variant_data['Price'] = attrs.get('price_each')
            # Check for specials
            special_price = attrs.get('special_price_each', {}).get('discount_price')
            if special_price:
                variant_data['Price'] = float(special_price)
                
            variant_data['Weight_Str'] = "Each"
            variant_data['Weight'] = None
            product_variants.append(variant_data)
        return product_variants # Return what we have

    # Loop through the weights that are actually available
    for weight_str in available_weights:
        # e.g., "gram" -> "price_gram"
        price_field = f"price_{weight_str.replace(' ', '_')}"
        # e.g., "gram" -> "special_price_gram"
        special_price_field = f"special_price_{weight_str.replace(' ', '_')}"

        price = attrs.get(price_field)
        
        # Check for a special price
        special_price_data = attrs.get(special_price_field, {})
        if special_price_data and special_price_data.get('discount_price'):
            price = float(special_price_data['discount_price'])

        if not price:
            continue # Skip if no price for this weight

        variant_data = common_data.copy()
        variant_data['Price'] = float(price)
        variant_data['Weight_Str'] = weight_str
        variant_data['Weight'] = convert_to_grams(weight_str)
        product_variants.append(variant_data)
            
    return product_variants

def fetch_iheartjane_data(store_id, store_name):
    """
    Fetches all product data for a specific iHeartJane store ID
    using the new v2/smart API.
    """
    print(f"Fetching data for iHeartJane store: {store_name} (ID: {store_id})...")
    
    all_products = []
    
    # These are the URL parameters, including our new API key
    params = {
        'jdm_api_key': NEW_JANE_API_KEY,
        'jdm_source': 'monolith',
        'jdm_version': '2.12.0'
    }
    
    # This is the full list of facets the browser requested
    # We will send the same list to look like a real request
    full_search_facets = [
        "activities", "aggregate_rating", "applicable_special_ids",
        "available_weights", "brand_subtype", "brand", "bucket_price",
        "category", "feelings", "has_brand_discount", "kind",
        "percent_cbd", "percent_thc", "root_types", "compound_names"
    ]

    # This payload now much more closely matches the real browser request
    payload = {
        "app_mode": "embedded",
        "jane_device_id": "me7dtQx8hW9YlcYmnHPys", # Static ID from your request
        "search_attributes": ["*"],
        "store_id": store_id,
        "disable_ads": False,
        "num_columns": 1,
        "page_size": 60,
        "page": 0,
        "placement": "menu_inline_table",
        "search_facets": full_search_facets, # Use the full list
        "search_filter": f"store_id = {store_id}",
        "search_query": "", # Empty query to get ALL products
        "search_sort": "recommendation"
    }

    try:
        # Make the POST request
        response = requests.post(NEW_JANE_URL, params=params, json=payload)
        response.raise_for_status() # Raise an error for bad responses
        
        data = response.json()
        hits = data.get('products', [])

        print(f"  ...retrieved {len(hits)} products in a single call.")

        # Process each product hit
        for hit in hits:
            product_variants = parse_jane_product(hit, store_name)
            all_products.extend(product_variants)
            
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for store {store_id}: {e}")
            
    if all_products:
        print(f"Successfully fetched {len(all_products)} product variants for {store_name}.")
        # Convert the list of dictionaries into a pandas DataFrame
        return pd.DataFrame(all_products)
    else:
        print(f"No data fetched for {store_name}.")
        return pd.DataFrame()

