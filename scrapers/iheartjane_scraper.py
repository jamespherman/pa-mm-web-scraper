# scrapers/iheartjane_scraper.py
# This is the iHeartJane scraper.

import requests
import pandas as pd
from .scraper_utils import convert_to_grams # Import our new util function

# --- Constants from your MATLAB code ---
ALGOLIA_URL = "https://VFM4X0N23A-dsn.algolia.net/1/indexes/menu-products-production/query"
ALGOLIA_HEADERS = {
    'x-algolia-api-key': 'edc5435c65d771cecbd98bbd488aa8d3',
    'x-algolia-application-id': 'VFM4X0N23A',
}

# Define the terpenes we want to extract
TERPENE_LIST = [
    'beta-Myrcene', 'Limonene', 'beta-Caryophyllene', 'Terpinolene',
    'Linalool', 'alpha-Pinene', 'beta-Pinene', 'Caryophyllene Oxide',
    'Guaiol', 'Humulene', 'alpha-Bisabolol', 'Camphene', 'Ocimene'
]

def parse_lab_data(lab_results):
    """
    Parses the 'lab_results' list from a product hit
    and returns a clean dictionary of cannabinoids and terpenes.
    """
    data = {}
    if not lab_results:
        return data

    # Extract cannabinoids
    data['THC'] = lab_results.get('THC', {}).get('value')
    data['CBD'] = lab_results.get('CBD', {}).get('value')
    data['THCa'] = lab_results.get('THCA', {}).get('value')
    
    # Extract terpenes
    terpenes = lab_results.get('Terpenes', {})
    if terpenes:
        total_terps = 0
        for terp_name in TERPENE_LIST:
            # Terpene names in the API might not match our list exactly
            # We'll try to find a close match
            for api_terp_name, terp_data in terpenes.items():
                if terp_name.lower() in api_terp_name.lower():
                    value = terp_data.get('value')
                    data[terp_name] = value
                    if value:
                        total_terps += value
                    break # Found our match
        data['Total_Terps'] = total_terps
        
    return data

def parse_iheartjane_product(hit, store_name):
    """
    Parses a single product 'hit' from the Algolia response
    and returns a list of dictionaries (one for each weight variant).
    """
    product_variants = []
    
    # 1. Get common data for this product
    common_data = {
        'Name': hit.get('name'),
        'Brand': hit.get('brand'),
        'Type': hit.get('kind'),
        'Subtype': hit.get('kind_subtype'),
        'Store': store_name,
    }

    # 2. Get lab data
    lab_data = parse_lab_data(hit.get('lab_results'))
    common_data.update(lab_data) # Add lab data to the common data

    # 3. Process price/weight variants
    # The 'prices' dict maps weight to price (e.g., {"Gram": 15, "Eighth Ounce": 45})
    if 'prices' in hit and hit['prices']:
        for weight_str, price in hit['prices'].items():
            if not price: # Skip if price is missing
                continue

            variant_data = common_data.copy()
            variant_data['Price'] = float(price)
            variant_data['Weight_Str'] = weight_str
            # Use our utility function to standardize the weight
            variant_data['Weight'] = convert_to_grams(weight_str)
            
            product_variants.append(variant_data)
            
    # Handle products with no 'prices' dict (e.g., some "Each" items)
    elif hit.get('price_each'):
        variant_data = common_data.copy()
        variant_data['Price'] = float(hit['price_each'])
        variant_data['Weight_Str'] = "Each"
        variant_data['Weight'] = None # Can't determine grams for "Each"
        product_variants.append(variant_data)

    return product_variants

def fetch_iheartjane_data(store_id, store_name):
    """
    Fetches all product data for a specific iHeartJane store ID.
    """
    print(f"Fetching data for iHeartJane store: {store_name} (ID: {store_id})...")
    
    all_products = []
    current_page = 0
    
    while True:
        # This is the payload (data) we send with our request
        payload = {
            "query": "",
            "filters": f"store_id:{store_id}",
            "hitsPerPage": 1000, # Get max hits per page
            "page": current_page,
            "facets": ["*"]
        }
        
        try:
            # Make the POST request to the Algolia API
            response = requests.post(ALGOLIA_URL, headers=ALGOLIA_HEADERS, json=payload)
            response.raise_for_status() # Raise an error for bad responses (4xx, 5xx)
            
            data = response.json()
            hits = data.get('hits', [])
            
            if not hits:
                print("No more hits found.")
                break # Exit the loop if no more products
                
            print(f"  ...retrieved page {current_page} with {len(hits)} products.")

            # Process each product 'hit' from the response
            for hit in hits:
                product_variants = parse_iheartjane_product(hit, store_name)
                all_products.extend(product_variants) # Add the list of variants

            # Check if we are on the last page
            if data.get('page') >= data.get('nbPages', 0) - 1:
                print("All pages processed.")
                break
                
            current_page += 1
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for store {store_id}: {e}")
            break # Exit loop on error
            
    if all_products:
        print(f"Successfully fetched {len(all_products)} product variants for {store_name}.")
        # Convert the list of dictionaries into a pandas DataFrame
        return pd.DataFrame(all_products)
    else:
        print(f"No data fetched for {store_name}.")
        return pd.DataFrame()