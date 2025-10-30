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

def parse_terpenes_from_text(text):
    """
    Uses regular expressions (regex) to find all terpenes in
    a block of text (like the 'store_notes' field).
    """
    terp_data = {}
    if not text:
        return terp_data
    
    # This pattern looks for "Name : 1.234%"
    # (?:...) is a non-capturing group
    # ([\w-]+) captures the terpene name (e.g., "beta-Caryophyllene")
    # [\s:]* matches the space or colon after the name
    # ([\d\.]+) captures the number (e.g., "1.234")
    pattern = r"([\w-]+)[\s:]*([\d\.]+)%"
    
    matches = re.findall(pattern, text, re.IGNORECASE)
    
    total_terps = 0
    for name, value in matches:
        # Check if this is a terpene we're looking for
        for official_name in TERPENE_LIST:
            if official_name.lower() in name.lower():
                val = float(value)
                terp_data[official_name] = val
                total_terps += val
                break # Found it, move to the next match
    
    if total_terps > 0:
        terp_data['Total_Terps'] = total_terps
        
    return terp_data

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
        'THC': attrs.get('percent_thc'), # Simpler!
        'THCa': attrs.get('percent_thca'),
        'CBD': attrs.get('percent_cbd'),
    }

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
    current_page = 0
    
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

    while True:
        # This payload now much more closely matches the real browser request
        payload = {
            "app_mode": "embedded",
            "jane_device_id": "me7dtQx8hW9YlcYmnHPys", # Static ID from your request
            "search_attributes": ["*"],
            "store_id": store_id,
            "disable_ads": False,
            "num_columns": 1,
            "page_size": 60,
            "page": current_page,
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
            
            if not hits:
                print("  ...No more hits found.")
                break # Exit the loop
                
            print(f"  ...retrieved page {current_page} with {len(hits)} products.")

            # Process each product hit
            for hit in hits:
                product_variants = parse_jane_product(hit, store_name)
                all_products.extend(product_variants)

            # Check if this was the last page
            if len(hits) < payload['page_size']:
                print("  ...All pages processed (last page was not full).")
                break
                
            current_page += 1
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for store {store_id}: {e}")
            break 
            
    if all_products:
        print(f"Successfully fetched {len(all_products)} product variants for {store_name}.")
        # Convert the list of dictionaries into a pandas DataFrame
        return pd.DataFrame(all_products)
    else:
        print(f"No data fetched for {store_name}.")
        return pd.DataFrame()

