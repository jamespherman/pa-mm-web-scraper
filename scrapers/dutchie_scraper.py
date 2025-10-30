# scrapers/dutchie_scraper.py
# This scraper fetches data from the Dutchie GraphQL API.

import requests
import pandas as pd
import numpy as np
from .scraper_utils import convert_to_grams

# --- Constants ---
API_URL = "https://dutchie.com/graphql"

HEADERS = {
    "Content-Type": "application/json",
    "x-dutchie-session": "eyJpZCI6ImFhZjk1MGY3LWM3ZTItNDQwMC1hNWM2LWU1ZGU2MDhmMWE3ZSIsImV4cGlyZXMiOjE3NjI0NDYwMzM3MDZ9"
}

STORES = {
    "Trulieve (Squirrel Hill)": "627429962262fd6c7c3dbbb2",
    "Trulieve (North Shore)": "6090306bd43b6e00c28cf0e5",
    "Ethos (Harmar)": "621900cebbc5580e15476deb"
}

CATEGORIES = ["Flower", "Vaporizers", "Concentrate"]

# Define known terpenes to look for
KNOWN_TERPENES = [
    'beta-Myrcene', 'Limonene', 'beta-Caryophyllene', 'Terpinolene',
    'Linalool', 'alpha-Pinene', 'beta-Pinene', 'Caryophyllene Oxide',
    'Guaiol', 'Humulene', 'alpha-Bisabolol', 'Camphene', 'Ocimene'
]

def get_all_product_slugs():
    """
    Step 1: Fetch all product cNames (slugs) for all stores and categories.
    """
    all_products = []
    print("Step 1: Fetching all product slugs...")

    for store_name, store_id in STORES.items():
        for category in CATEGORIES:
            page = 0
            while True:
                payload = {
                    "operationName": "FilteredProducts",
                    "variables": {
                        "dispensaryId": store_id,
                        "types": [category],
                        "page": page,
                        "perPage": 50
                    },
                    "extensions": {
                        "persistedQuery": {
                            "version": 1,
                            "sha256Hash": "f3b8a3820696e2a6d06487ebb4d86df454a90ed863185c1a659e7a126f391644"
                        }
                    }
                }
                try:
                    response = requests.post(API_URL, headers=HEADERS, json=payload)
                    response.raise_for_status()
                    products = response.json()['data']['filteredProducts']['products']

                    if not products:
                        break  # Exit loop if no more products

                    for product in products:
                        all_products.append({
                            "cName": product['cName'],
                            "DispensaryID": store_id,
                            "StoreName": store_name
                        })

                    page += 1
                except requests.exceptions.RequestException as e:
                    print(f"Error fetching product slugs for {store_name} ({category}): {e}")
                    break  # Stop trying for this store/category on error

    print(f"  ...found {len(all_products)} total product slugs.")
    return all_products

def get_detailed_product_info(product_slugs):
    """
    Step 2 & 3: Fetch detailed info for each product and parse the data.
    """
    all_product_data = []
    print("\nStep 2: Fetching detailed product information...")

    for i, slug_info in enumerate(product_slugs):
        cName = slug_info['cName']
        dispensaryId = slug_info['DispensaryID']
        store_name = slug_info['StoreName']

        if (i + 1) % 50 == 0:
            print(f"  ...processing product {i + 1}/{len(product_slugs)}")

        payload = {
            "operationName": "IndividualFilteredProduct",
            "variables": {
                "cName": cName,
                "dispensaryId": dispensaryId,
                "includeTerpenes": True,
                "includeCannabinoids": True
            },
            "extensions": {
                "persistedQuery": {
                    "version": 1,
                    "sha256Hash": "7e321b76b73d96861462a1c4f676cab46e7a0745f6ac63538498d51f0aae1507"
                }
            }
        }
        try:
            response = requests.post(API_URL, headers=HEADERS, json=payload)
            response.raise_for_status()
            products = response.json()['data']['filteredProducts']['products']

            if not products:
                continue

            product = products[0]
            parsed_data = parse_product_details(product, store_name)
            if parsed_data:
                all_product_data.append(parsed_data)

        except requests.exceptions.RequestException as e:
            print(f"Error fetching details for {cName}: {e}")

    print(f"  ...successfully parsed {len(all_product_data)} products.")
    return all_product_data

def parse_product_details(product, store_name):
    """
    Parses the detailed product JSON into a flat dictionary.
    """
    data = {
        'Name': product.get('Name', 'N/A'),
        'Brand': product.get('brandName', 'N/A'),
        'Type': product.get('type', 'N/A'),
        'Subtype': product.get('subcategory', 'N/A'),
        'Store': store_name
    }

    # --- Price ---
    prices = product.get('medicalPrices', []) + product.get('medicalSpecialPrices', [])
    data['Price'] = min(prices) if prices else np.nan

    # --- Weight ---
    # This logic assumes the first option is the primary one.
    options = product.get('Options', [])
    weight_str = options[0] if options else None
    data['Weight'] = convert_to_grams(weight_str)

    # --- Cannabinoids ---
    cannabinoid_data = {}
    if product.get('cannabinoids'):
        for cannabinoid in product['cannabinoids']:
            cannabinoid_data[cannabinoid['name']] = cannabinoid['value']

    # Fallback for THC/CBD
    thc = product.get('THCContent', {}).get('range', [None])[0]
    cbd = product.get('CBDContent', {}).get('range', [None])[0]
    if 'THC' not in cannabinoid_data and thc is not None:
        cannabinoid_data['THC'] = thc
    if 'CBD' not in cannabinoid_data and cbd is not None:
        cannabinoid_data['CBD'] = cbd
    data.update(cannabinoid_data)

    # --- Terpenes ---
    terpene_data = {terp: 0 for terp in KNOWN_TERPENES}
    total_terps = 0
    if product.get('terpenes'):
        for terp in product['terpenes']:
            name = terp['libraryTerpene']['name']
            value = terp.get('value')
            if value is not None:
                # Basic standardization, can be improved
                standard_name = next((known_terp for known_terp in KNOWN_TERPENES if known_terp.lower() in name.lower()), None)
                if standard_name:
                    terpene_data[standard_name] = value
                    total_terps += value

    data.update(terpene_data)
    data['Total_Terps'] = total_terps

    return data

def get_dutchie_data():
    """
    Main function to orchestrate the Dutchie scraping process.
    """
    # Step 1: Get all product slugs
    product_slugs = get_all_product_slugs()

    # Step 2 & 3: Get detailed info and parse it
    product_details = get_detailed_product_info(product_slugs)

    if not product_details:
        print("No product data was fetched. Returning an empty DataFrame.")
        return pd.DataFrame()

    # Step 4: Final DataFrame
    df = pd.DataFrame(product_details)

    # Calculate DPG
    df['dpg'] = df['Price'] / df['Weight']

    # Define column order
    cannabinoid_cols = sorted([col for col in df.columns if col not in KNOWN_TERPENES + ['Name', 'Store', 'Brand', 'Type', 'Subtype', 'Weight', 'Price', 'dpg', 'Total_Terps']])
    terpene_cols = KNOWN_TERPENES

    column_order = (
        ['Name', 'Store', 'Brand', 'Type', 'Subtype', 'Weight', 'Price', 'dpg', 'Total_Terps'] +
        cannabinoid_cols +
        terpene_cols
    )

    # Reorder and fill NaNs
    df = df.reindex(columns=column_order).fillna(0)

    print("\nScraping complete. DataFrame created.")
    return df
