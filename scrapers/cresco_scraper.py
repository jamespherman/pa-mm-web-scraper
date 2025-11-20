# scrapers/cresco_scraper.py
# -----------------------------------------------------------------------------
# This scraper handles the Cresco / Sunnyside dispensary website.
#
# It works by sending requests to their "hidden" API (api.crescolabs.com).
# This is much faster and more reliable than trying to click through the website
# with a browser bot.
#
# Key challenges here:
# 1. The API requires specific headers (like 'x-api-key') to work.
# 2. Data is paginated (split into multiple pages), so we need a loop.
# -----------------------------------------------------------------------------

import requests # Used to send internet requests (like a browser does).
import pandas as pd # Used to organize data into tables.
import numpy as np # Used for math operations (like calculating 'NaN' for empty numbers).
from .scraper_utils import (
    convert_to_grams, BRAND_MAP, MASTER_CATEGORY_MAP,
    MASTER_SUBCATEGORY_MAP, MASTER_COMPOUND_MAP, save_raw_json
)
import re # Regular expressions for text patterns.

# --- Constants ---
# The main address for the API. We found this by inspecting the "Network" tab
# in the browser's developer tools while browsing the site.
BASE_URL = "https://api.crescolabs.com/p/inventory/op/fifo-inventory"

# --- Headers ---
# Websites check "headers" to see if a request comes from a real browser or a bot.
# We copy these from a real browser session to "trick" the API into talking to us.
# Note: We do NOT use the 'authorization' header because it expires too fast.
# The 'x-api-key' seems to stay the same for longer.
HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-US,en;q=0.9,it-IT;q=0.8,it;q=0.7",
    "ordering_app_id": "9ha3c289-1260-4he2-nm62-4598bca34naa", # Unique ID for the web app
    "origin": "https://www.sunnyside.shop",
    "referer": "https://www.sunnyside.shop/",
    "sec-ch-ua": "\"Microsoft Edge\";v=\"141\", \"Not?A_Brand\";v=\"8\", \"Chromium\";v=\"141\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\"",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "cross-site",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0",
    "x-api-key": "hE1gQuwYcO54382jYNH0c9W0w4fEC3dJ8ljnwVau", # The key that lets us in!
    "x-client-version": "4.20.0"
}

# The categories we are interested in scraping.
CATEGORIES = ["flower", "vapes", "concentrates"]

def parse_cresco_products(products, store_name):
    """
    This function takes the raw list of products from the API and cleans it up.
    It extracts only the info we care about (Name, Price, THC, etc.) and
    puts it into a nice dictionary format.

    Args:
        products (list): A list of raw product dictionaries from the API.
        store_name (str): The name of the store these products belong to.

    Returns:
        list: A list of cleaned-up product dictionaries.
    """
    parsed_products = []

    for product in products:
        
        # --- 1. Category Standardization ---
        # The API might call it "flower-3.5g", but we just want "Flower".
        # We look inside the nested 'sku' -> 'product' -> 'category' fields.
        category_name = product.get('sku', {}).get('product', {}).get('category')

        # Check our MASTER_CATEGORY_MAP to see if we recognize this category.
        standardized_category = MASTER_CATEGORY_MAP.get(category_name)

        # If we don't recognize it (e.g., "Accessories"), we skip this product.
        if not standardized_category:
            continue

        # --- 2. Brand and Subcategory Standardization ---
        brand_name = product.get('brand', 'N/A')
        sub_category_name = product.get('sku', {}).get('product', {}).get('sub_category')

        # Build the main data dictionary
        data = {
            'Name': product.get('name', 'N/A'),
            # Use our map to fix brand names (e.g., "Cresco™" -> "Cresco")
            'Brand': BRAND_MAP.get(brand_name, brand_name),
            'Store': store_name,
            'Type': standardized_category,
            'Subtype': MASTER_SUBCATEGORY_MAP.get(sub_category_name, sub_category_name)
        }

        # --- 3. Pricing and Weight ---
        # Prefer the discounted price if it exists, otherwise use regular price.
        price = product.get('discounted_price') or product.get('price')

        # Convert price to a float number. If it's missing, use 'NaN' (Not a Number).
        data['Price'] = float(price) if price is not None else np.nan

        # Get the weight string (e.g., "3.5g") directly from the API
        data['Weight_Str'] = product.get('sku', {}).get('product', {}).get('weight')

        # The API also provides weight in grams directly, which is convenient!
        data['Weight'] = product.get('sku', {}).get('product', {}).get('weight_in_g')

        # --- 4. Compounds (THC, CBD, Terpenes) ---
        # The 'potency' field contains a dictionary of chemicals.
        compounds_dict = {}
        potency_dict = product.get('potency', {})

        if potency_dict:
            for key, value in potency_dict.items():
                # Check if we know this chemical name (e.g., "thca" -> "THCa")
                standard_name = MASTER_COMPOUND_MAP.get(key)
                if standard_name:
                    compounds_dict[standard_name] = value

        # Add the found compounds to our product data
        data.update(compounds_dict)

        parsed_products.append(data)

    return parsed_products

def fetch_cresco_data(stores):
    """
    This is the main function that controls the Cresco scraping job.
    It loops through every store and every category to get all the data.
    """
    all_products_list = [] # We will add all found products to this big list
    print("Starting Cresco (Sunnyside) Scraper (api.crescolabs.com)...")

    # Loop through each store provided in the 'stores' dictionary
    for store_name, store_id in stores.items():
        print(f"Fetching data for Sunnyside store: {store_name} (ID: {store_id})...")

        # Create headers specific to this store
        headers = HEADERS.copy()
        headers['store_id'] = store_id
        
        # Loop through each category (Flower, Vapes, etc.)
        for category in CATEGORIES:
            page = 0
            limit = 50 # The API gives us 50 items at a time
            total_scraped = 0

            # Loop through pages of results until there are no more
            while True:
                try:
                    # These parameters tell the API exactly what we want.
                    params = {
                        'category': category,
                        'inventory_type': 'retail',
                        'require_sellable_quantity': 'true', # Only in-stock items
                        'include_specials': 'true',
                        'sellable': 'true',
                        'order_by': 'brand',
                        'limit': str(limit),
                        'usage_type': 'medical',
                        'hob_first': 'true',
                        'include_filters': 'true',
                        'include_facets': 'true',
                        'offset': str(page * limit) # This skips items we've already seen
                    }

                    # Send the request to the API
                    response = requests.get(BASE_URL, headers=headers, params=params, timeout=10)
                    response.raise_for_status() # Check for errors (like 404 Not Found)
                    json_response = response.json() # Convert response to JSON

                    # --- Save Raw Data ---
                    # We save the exact response to a file for debugging/backup.
                    filename_parts = ['cresco', store_name, category, f'p{page}']
                    save_raw_json(json_response, filename_parts)

                    # Get the list of products from the response
                    products = json_response.get('data')
                    
                    # If the list is empty, we are done with this category.
                    if not products:
                        print(f"  ...completed category: {category}. Found {total_scraped} products.")
                        break
                        
                    # Parse the products and add them to our big list
                    parsed_products = parse_cresco_products(products, store_name)
                    all_products_list.extend(parsed_products)
                    total_scraped += len(parsed_products)
                    
                    # If we got fewer items than the limit (50), it means we reached the end.
                    if len(products) < limit:
                        print(f"  ...completed category: {category}. Found {total_scraped} products.")
                        break

                    # Go to the next page
                    page += 1

                except requests.exceptions.RequestException as e:
                    print(f"Error fetching page {page} for {category} at {store_name}: {e}")
                    break
                except Exception as e:
                    print(f"An error occurred processing page {page} for {category}: {e}")
                    break

    # If we found nothing at all, return an empty table.
    if not all_products_list:
        print("No product data was fetched from Cresco. Returning an empty DataFrame.")
        return pd.DataFrame()

    # Convert our list of dictionaries into a pandas DataFrame
    df = pd.DataFrame(all_products_list)

    # Calculate Dollars Per Gram (dpg)
    df['dpg'] = df['Price'] / df['Weight']

    print(f"\nScraping complete for Cresco. DataFrame created with {len(df)} rows.")
    return df
