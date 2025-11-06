# scrapers/dutchie_scraper.py
# This scraper is designed to fetch data from the Dutchie GraphQL API.
# Dutchie is a common platform for cannabis dispensaries, but each dispensary
# often has its own unique API endpoint and configuration. This scraper is
# built to handle these variations by using a detailed configuration dictionary.

import requests
import pandas as pd
import numpy as np
import json
from .scraper_utils import convert_to_grams

# --- Constants ---

# The DUTCHIE_STORES dictionary is the core configuration for this scraper.
# Each key is a user-friendly store name, and the value is a dictionary
# containing the specific details needed to interact with that store's API.
#
# - `api_url`: The specific GraphQL endpoint for the dispensary.
# - `store_id`: The unique identifier for the specific store location on the Dutchie platform.
# - `headers`: A dictionary of HTTP headers. These are often crucial for mimicking
#   a legitimate browser request to avoid being blocked. The `referer` and
#   `x-dutchie-session` headers can be particularly important.
DUTCHIE_STORES = {
    "Ethos (Harmar)": {
        "api_url": "https://harmarville.ethoscannabis.com/api-4/graphql",
        "store_id": "621900cebbc5580e15476deb",
        "headers": {
            "accept": "*/*",
            "apollographql-client-name": "Marketplace (production)",
            "content-type": "application/json",
            "referer": "https://harmarville.ethoscannabis.com/stores/ethos-harmarville",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0",
            "x-dutchie-session": "eyJpZCI6ImM1MDc2NGI5LTAyZWUtNDU2ZS05ODc0LTZmNzkyOTQwYzc2NiIsImV4cGlyZXMiOjE3NjI0Mjc5MTU5NzR9"
        }
    },
    "Ethos (Pleasant Hills)": {
        "api_url": "https://pittsburgh.ethoscannabis.com/api-4/graphql",
        "store_id": "607dc27bfde18500b5e8dd52",
        "headers": {
            "accept": "*/*",
            "apollographql-client-name": "Marketplace (production)",
            "content-type": "application/json",
            "referer": "https://pleasanthills.ethoscannabis.com/stores/ethos-pleasant-hills",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0",
            "x-dutchie-session": "eyJpZCI6ImM1MDc2NGI5LTAyZWUtNDU2ZS05ODc0LTZmNzkyOTQwYzc2NiIsImV4cGlyZXMiOjE3NjI0Mjc5MTU5NzR9"
        }
    },
    "Ethos (North Fayette)": {
        "api_url": "https://pittsburgh.ethoscannabis.com/api-4/graphql",
        "store_id": "5fa0829005bb2400cfc4b694",
        "headers": {
            "accept": "*/*",
            "apollographql-client-name": "Marketplace (production)",
            "content-type": "application/json",
            "referer": "https://northfayette.ethoscannabis.com/stores/ethos-north-fayette",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0",
            "x-dutchie-session": "eyJpZCI6ImM1MDc2NGI5LTAyZWUtNDU2ZS05ODc0LTZmNzkyOTQwYzc2NiIsImV4cGlyZXMiOjE3NjI0Mjc5MTU5NzR9"
        }
    },
#    "Liberty (PGH)": {
#        "api_url": "https://dutchie.com/api-1/graphql",
#        "store_id": "63dab2d8ab202100548dbaf5",
#        "headers": {
#            "accept": "*/*",
#            "apollographql-client-name": "Marketplace (production)",
#            "content-type": "application/json",
#            "referer": "https://dutchie.com/embedded-menu/liberty-pittsburgh",
#            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0",
#            "x-dutchie-session": "eyJpZCI6ImFiMGRlMWMyLWNhNWUtNGM4Zi05NDA1LTRhZWQ5MGNhZTQzOCIsImV4cGlyZXMiOjE3NjI0ODE2MDE3Mjd9"
#        }
#    },
    "Ascend (Cranberry)": {
        "api_url": "https://letsascend.com/api-4/graphql",
        "store_id": "66fef50576b5d1b3703a1890",
        "headers": {
            "accept": "*/*",
            "apollographql-client-name": "Marketplace (production)",
            "content-type": "application/json",
            "referer": "https://letsascend.com/stores/cranberry-pennsylvania",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15",
            "x-dutchie-session": "eyJpZCI6IjNhMTFmZGZhLTU5MGQtNDk5ZC1hYzE4LTRjNjhlZjRjNjZkNiIsImV4cGlyZXMiOjE3NjI0ODA3NzY0ODF9"
        }
    },
    "Ascend (Monaca)": {
        "api_url": "https://letsascend.com/api-4/graphql",
        "store_id": "66fef58038ff55ae0d700b55",
        "headers": {
            "accept": "*/*",
            "apollographql-client-name": "Marketplace (production)",
            "content-type": "application/json",
            "referer": "https://letsascend.com/stores/monaca-pennsylvania",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15",
            "x-dutchie-session": "eyJpZCI6IjNhMTFmZGZhLTU5MGQtNDk5ZC1hYzE4LTRjNjhlZjRjNjZkNiIsImV4cGlyZXMiOjE3NjI0ODA3NzY0ODF9"
        }
    }
}

# A predefined list of known terpenes to ensure the final DataFrame has a consistent structure.
KNOWN_TERPENES = [
    'beta-Myrcene', 'Limonene', 'beta-Caryophyllene', 'Terpinolene',
    'Linalool', 'alpha-Pinene', 'beta-Pinene', 'Caryophyllene Oxide',
    'Guaiol', 'Humulene', 'alpha-Bisabolol', 'Camphene', 'Ocimene'
]

# A mapping to standardize terpene names. APIs often return variations
# (e.g., 'b-myrcene', 'beta-myrcene'), and this ensures they are all mapped
# to a single, canonical name.
TERPENE_MAPPING = {
    'alpha-pinene': 'alpha-Pinene',
    'a-pinene': 'alpha-Pinene',
    'pinene': 'alpha-Pinene',
    'beta-pinene': 'beta-Pinene',
    'b-pinene': 'beta-Pinene',
    'beta-myrcene': 'beta-Myrcene',
    'myrcene': 'beta-Myrcene',
    'b-myrcene': 'beta-Myrcene',
    'limonene': 'Limonene',
    'd-limonene': 'Limonene',
    'beta-caryophyllene': 'beta-Caryophyllene',
    'caryophyllene': 'beta-Caryophyllene',
    'b-caryophyllene': 'beta-Caryophyllene',
    'linalool': 'Linalool',
    'terpinolene': 'Terpinolene',
    'humulene': 'Humulene',
    'alpha-humulene': 'Humulene',
    'a-humulene': 'Humulene',
    'ocimene': 'Ocimene',
    'beta-ocimene': 'Ocimene',
    'b-ocimene': 'Ocimene',
    'guaiol': 'Guaiol',
    'alpha-bisabolol': 'alpha-Bisabolol',
    'bisabolol': 'alpha-Bisabolol',
    'a-bisabolol': 'alpha-Bisabolol',
    'camphene': 'Camphene',
    'caryophyllene oxide': 'Caryophyllene Oxide',
}

def get_all_product_slugs(store_name, store_config):
    """
    Fetches the unique identifiers (`cName` or slugs) for all products in a store.

    This function paginates through the dispensary's menu, collecting the `cName`
    for every product. This is the first step, as these slugs are required to
    fetch the detailed information for each product.

    Args:
        store_name (str): The name of the store being scraped.
        store_config (dict): The configuration dictionary for the store.

    Returns:
        list: A list of dictionaries, each containing a product's `cName` and store info.
    """
    all_products = []
    print(f"Step 1: Fetching product slugs for {store_name}...")
    
    api_url = store_config['api_url']
    store_id = store_config['store_id']
    headers = store_config['headers']

    page = 0
    while True:
        # This is the GraphQL payload. It's sent as URL parameters.
        # `extensions.persistedQuery.sha256Hash` is a key part of Dutchie's API.
        # Instead of sending the full query text, the client sends a hash of the query.
        # If this hash ever changes on their backend, this scraper will break.
        variables = {
            "includeEnterpriseSpecials": False,
            "productsFilter": {
                "dispensaryId": store_id,
                "pricingType": "med",
                "strainTypes": [],
                "subcategories": [],
                "Status": "Active",
                "types": [],
                "useCache": False,
                "isDefaultSort": False,
                "sortBy": "relevance",
                "sortDirection": 1,
                "bypassOnlineThresholds": False,
                "isKioskMenu": False,
                "removeProductsBelowOptionThresholds": True
            },
            "page": page,
            "perPage": 100
        }

        extensions = {
            "persistedQuery": {
                "version": 1,
                "sha256Hash": "ee29c060826dc41c527e470e9ae502c9b2c169720faa0a9f5d25e1b9a530a4a0"
            }
        }

        params = {
            'operationName': 'FilteredProducts',
            'variables': json.dumps(variables),
            'extensions': json.dumps(extensions)
        }

        try:
            response = requests.get(api_url, headers=headers, params=params)
            response.raise_for_status()
            
            json_response = response.json()
            if 'errors' in json_response:
                print(f"GraphQL Error in product slugs for {store_name}: {json_response['errors']}")
                break
                
            products = json_response['data']['filteredProducts']['products']

            if not products:
                break  # Exit loop if no more products

            for product in products:
                all_products.append({
                    "cName": product['cName'],
                    "DispensaryID": store_id,
                    "StoreName": store_name,
                    "StoreConfig": store_config # Pass config for detail request
                })
            page += 1
        except requests.exceptions.RequestException as e:
            print(f"Error fetching product slugs for {store_name}: {e}")
            break
        except KeyError:
            print(f"Unexpected JSON structure for {store_name}. Skipping.")
            print(f"Response was: {response.text}")
            break

    print(f"  ...found {len(all_products)} total product slugs for {store_name}.")
    return all_products

def get_detailed_product_info(product_slugs):
    """
    Fetches and parses detailed information for a list of product slugs.

    This function iterates through the collected slugs, makes a new API call for
    each one to get detailed data (terpenes, cannabinoids, price), and then
    parses the complex JSON response into a simple, flat dictionary.

    Args:
        product_slugs (list): A list of product slug dictionaries from `get_all_product_slugs`.

    Returns:
        list: A list of dictionaries, where each dictionary represents a parsed product.
    """
    all_product_data = []
    print("\nStep 2: Fetching detailed product information from Dutchie...")

    for i, slug_info in enumerate(product_slugs):
        cName = slug_info['cName']
        dispensaryId = slug_info['DispensaryID']
        store_name = slug_info['StoreName']
        store_config = slug_info['StoreConfig']

        if (i + 1) % 50 == 0:
            print(f"  ...processing product {i + 1}/{len(product_slugs)}")
        # This is the second GraphQL query, for individual product details.
        # Note the different `sha256Hash`.
        variables = {
            "includeTerpenes": True,
            "includeCannabinoids": True,
            "includeEnterpriseSpecials": False,
            "productsFilter": {
                "cName": cName,
                "dispensaryId": dispensaryId,
                "removeProductsBelowOptionThresholds": False,
                "isKioskMenu": False,
                "bypassKioskThresholds": False,
                "bypassOnlineThresholds": True,
                "Status": "All"
            }
        }
        
        extensions = {
            "persistedQuery": {
                "version": 1,
                "sha256Hash": "47369a02fc8256aaf1ed70d0c958c88514acdf55c5810a5be8e0ee1a19617cda"
            }
        }
        
        params = {
            'operationName': 'IndividualFilteredProduct',
            'variables': json.dumps(variables),
            'extensions': json.dumps(extensions)
        }
        
        try:
            response = requests.get(store_config['api_url'], headers=store_config['headers'], params=params)
            response.raise_for_status()
            
            json_response = response.json()
            if 'errors' in json_response:
                print(f"GraphQL Error in product details for {cName}: {json_response['errors']}")
                continue
                
            products = json_response['data']['filteredProducts']['products']

            if not products:
                continue

            product = products[0]
            parsed_data = parse_product_details(product, store_name)
            if parsed_data:
                all_product_data.append(parsed_data)

        except requests.exceptions.RequestException as e:
            print(f"Error fetching details for {cName}: {e}")
        except KeyError:
            print(f"Unexpected JSON structure for details {cName}. Skipping.")
            print(f"Response was: {response.text}")
            continue

    print(f"  ...successfully parsed {len(all_product_data)} products.")
    return all_product_data

def parse_product_details(product, store_name):
    """
    Parses the complex, nested JSON of a single product into a flat dictionary.

    This function handles the extraction of key information and standardizes it.
    It uses `.get()` extensively to avoid errors if a key is missing in the response.

    Args:
        product (dict): The JSON dictionary for a single product from the API.
        store_name (str): The name of the store.

    Returns:
        dict: A flattened dictionary containing the key product information.
    """
    data = {
        'Name': product.get('Name', 'N/A'),
        'Brand': product.get('brandName', 'N/A'),
        'Type': product.get('type', 'N/A'),
        'Subtype': product.get('subcategory', 'N/A'),
        'Store': store_name
    }

    # --- Price ---
    prices = product.get('medicalPrices', [])
    special_prices = product.get('medicalSpecialPrices', [])
    
    final_price = np.nan
    if special_prices:
        final_price = min(special_prices)
    elif prices:
        final_price = min(prices)
        
    data['Price'] = final_price

    # --- Weight ---
    options = product.get('Options', [])
    weight_str = options[0] if options else None
    data['Weight'] = convert_to_grams(weight_str)
    data['Weight_Str'] = weight_str if weight_str else 'N/A'

    # --- Cannabinoids ---
    cannabinoid_data = {}
    if product.get('cannabinoidsV2'): # Use the V2 field
        for cannabinoid in product['cannabinoidsV2']:
            if 'name' in cannabinoid and 'value' in cannabinoid:
                cannabinoid_data[cannabinoid['name']] = cannabinoid['value']

    thc_content = product.get('THCContent') or {}
    thc_range = thc_content.get('range', [])
    cbd_content = product.get('CBDContent') or {}
    cbd_range = cbd_content.get('range', [])
    
    if 'THC' not in cannabinoid_data and thc_range and thc_range[0] is not None:
        cannabinoid_data['THC'] = thc_range[0]
    if 'CBD' not in cannabinoid_data and cbd_range and cbd_range[0] is not None:
        cannabinoid_data['CBD'] = cbd_range[0]
        
    data.update(cannabinoid_data)

    # --- Terpenes ---
    terpene_data = {terp: np.nan for terp in KNOWN_TERPENES}
    total_terps = 0
    if product.get('terpenes'): 
        for terp in product['terpenes']:
            name = terp.get('name', terp.get('libraryTerpene', {}).get('name'))
            value = terp.get('value')
            if name and value is not None:
                clean_name = name.strip().lower()
                standard_name = TERPENE_MAPPING.get(clean_name)
                
                if standard_name:
                    terpene_data[standard_name] = value
                    total_terps += value

    data.update(terpene_data)
    data['Total_Terps'] = total_terps if total_terps > 0 else np.nan

    return data

def fetch_dutchie_data():
    """
    The main orchestration function for the Dutchie scraper.

    It calls the other functions in this module to get all product data from
    all configured Dutchie stores and returns it as a single DataFrame.

    Returns:
        pd.DataFrame: A DataFrame containing all scraped product data from Dutchie stores.
    """
    all_store_slugs = []
    
    # Loop over each store configuration
    for store_name, store_config in DUTCHIE_STORES.items():
        slugs = get_all_product_slugs(store_name, store_config)
        all_store_slugs.extend(slugs)

    if not all_store_slugs:
        print("No product slugs found for any Dutchie store. Exiting Dutchie scraper.")
        return pd.DataFrame()

    product_details = get_detailed_product_info(all_store_slugs)

    if not product_details:
        print("No product data was fetched. Returning an empty DataFrame.")
        return pd.DataFrame()

    # Step 4: Final DataFrame
    df = pd.DataFrame(product_details)

    # Calculate DPG
    df['dpg'] = df['Price'] / df['Weight']

    # Define column order
    cannabinoid_cols = sorted([col for col in df.columns if col not in KNOWN_TERPENES + ['Name', 'Store', 'Brand', 'Type', 'Subtype', 'Weight', 'Weight_Str', 'Price', 'dpg', 'Total_Terps']])
    terpene_cols = KNOWN_TERPENES

    column_order = (
        ['Name', 'Store', 'Brand', 'Type', 'Subtype', 'Weight', 'Weight_Str', 'Price', 'dpg', 'Total_Terps'] +
        cannabinoid_cols +
        terpene_cols
    )

    # Reorder and fill NaNs
    df = df.reindex(columns=column_order).fillna(np.nan)

    print("\nScraping complete for Dutchie stores. DataFrame created.")
    return df
