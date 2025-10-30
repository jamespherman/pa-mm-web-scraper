# scrapers/dutchie_scraper.py
# This scraper fetches data from the Dutchie GraphQL API.
# It is designed to handle multiple Dutchie configurations:
# 1. Proxy-based (e.g., Ethos)
# 2. Direct-based (e.g., Liberty)

import requests
import pandas as pd
import numpy as np
import json
from .scraper_utils import convert_to_grams

# --- Constants ---

# Define store configurations
# Each store needs its own API base, headers, and store ID
DUTCHIE_STORES = {
    "Ethos (Harmar)": {
        "api_url": "https://harmarville.ethoscannabis.com/api-4/graphql",
        "store_id": "621900cebbc5580e15476deb",
        "headers": {
            "accept": "*/*",
            "apollographql-client-name": "Marketplace (production)",
            "content-type": "application/json",
            "referer": "https://harmarville.ethoscannabis.com/stores/ethos-harmarville/products/flower",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0",
            "x-dutchie-session": "eyJpZCI6Ijk0YTNhOWU4LTZkNzctNDY2OC04MzFjLTA2YmFlOWU5ODdmYSIsImV4cGlyZXMiOjE3NjI0NTc3MTk4Mjl9"
        }
    },
    "Ethos (Pleasant Hills)": {
        "api_url": "https://harmarville.ethoscannabis.com/api-4/graphql",
        "store_id": "607dc27bfde18500b5e8dd52",
        "headers": {
            "accept": "*/*",
            "apollographql-client-name": "Marketplace (production)",
            "content-type": "application/json",
            "referer": "https://harmarville.ethoscannabis.com/stores/ethos-pleasant-hills",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0",
            "x-dutchie-session": "eyJpZCI6Ijk0YTNhOWU4LTZkNzctNDY2OC04MzFjLTA2YmFlOWU5ODdmYSIsImV4cGlyZXMiOjE3NjI0NTc3MTk4Mjl9"
        }
    },
    "Ethos (North Fayette)": {
        "api_url": "https://harmarville.ethoscannabis.com/api-4/graphql",
        "store_id": "5fa0829005bb2400cfc4b694",
        "headers": {
            "accept": "*/*",
            "apollographql-client-name": "Marketplace (production)",
            "content-type": "application/json",
            "referer": "https://harmarville.ethoscannabis.com/stores/ethos-north-fayette",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0",
            "x-dutchie-session": "eyJpZCI6Ijk0YTNhOWU4LTZkNzctNDY2OC04MzFjLTA2YmFlOWU5ODdmYSIsImV4cGlyZXMiOjE3NjI0NTc3MTk4Mjl9"
        }
    },
    "Liberty (PGH)": {
        "api_url": "https://dutchie.com/api-1/graphql",
        "store_id": "63dab2d8ab202100548dbaf5",
        "headers": {
            "accept": "*/*",
            "apollographql-client-name": "Marketplace (production)",
            "content-type": "application/json",
            "referer": "https://dutchie.com/embedded-menu/liberty-pittsburgh",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0",
            "x-dutchie-session": "eyJpZCI6IjFiMThiZjYwLTFlYjItNDM3MS1iNTQ0LTI5ZTUyYjUzMWM5ZiIsImV4cGlyZXMiOjE3NjI0NTk4NDQzODN9"
        }
    }
}

CATEGORIES = ["Flower", "Vaporizers", "Concentrate"]

KNOWN_TERPENES = [
    'beta-Myrcene', 'Limonene', 'beta-Caryophyllene', 'Terpinolene',
    'Linalool', 'alpha-Pinene', 'beta-Pinene', 'Caryophyllene Oxide',
    'Guaiol', 'Humulene', 'alpha-Bisabolol', 'Camphene', 'Ocimene'
]

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
    Step 1: Fetch all product cNames (slugs) for a specific store.
    """
    all_products = []
    print(f"Step 1: Fetching product slugs for {store_name}...")
    
    api_url = store_config['api_url']
    store_id = store_config['store_id']
    headers = store_config['headers']

    for category in CATEGORIES:
        page = 0
        while True:
            variables = {
                "includeEnterpriseSpecials": False,
                "productsFilter": {
                    "dispensaryId": store_id,
                    "pricingType": "med",
                    "strainTypes": [],
                    "subcategories": [],
                    "Status": "Active",
                    "types": [category],
                    "useCache": False,
                    "isDefaultSort": True,
                    "sortBy": "popularSortIdx",
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
                    "sha256Hash": "f3b8a3820696e2a6d06487ebb4d86df454a90ed863185c1a659e7a126f391644"
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
                    print(f"GraphQL Error in product slugs for {store_name} ({category}): {json_response['errors']}")
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
                print(f"Error fetching product slugs for {store_name} ({category}): {e}")
                break
            except KeyError:
                print(f"Unexpected JSON structure for {store_name} ({category}). Skipping.")
                print(f"Response was: {response.text}")
                break

    print(f"  ...found {len(all_products)} total product slugs for {store_name}.")
    return all_products

def get_detailed_product_info(product_slugs):
    """
    Step 2 & 3: Fetch detailed info for each product and parse the data.
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
                "sha256Hash": "7e321b76b73d96861462a1c4f676cab46e7a0745f6ac63538498d51f0aae1507"
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

    thc_range = product.get('THCContent', {}).get('range', [])
    cbd_range = product.get('CBDContent', {}).get('range', [])
    
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
                clean_name = name.strip().lower().replace('-', '')
                standard_name = TERPENE_MAPPING.get(clean_name)
                
                if standard_name:
                    terpene_data[standard_name] = value
                    total_terps += value

    data.update(terpene_data)
    data['Total_Terps'] = total_terps if total_terps > 0 else np.nan

    return data

def fetch_dutchie_data():
    """
    Main function to orchestrate the Dutchie scraping process.
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

