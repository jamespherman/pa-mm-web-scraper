# scrapers/trulieve_scraper.py
# This scraper fetches data from the new Trulieve v2 API.

import requests
import pandas as pd
import numpy as np
from .scraper_utils import convert_to_grams
import re

# --- Constants ---
BASE_URL = "https://api.trulieve.com/api/v2/menu/{store_id}/{category}/MEDICAL"
HEADERS = {
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.9",
    "origin": "https://www.trulieve.com",
    "referer": "https://www.trulieve.com/",
    "sec-ch-ua": "\"Microsoft Edge\";v=\"141\", \"Not?A_Brand\";v=\"8\", \"Chromium\";v=\"141\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\"",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0"
}

CATEGORIES = ["flower", "vaporizers", "concentrates", "tinctures", "edibles"]

# Define known terpenes to look for (copied from iheartjane)
KNOWN_TERPENES = [
    'beta-Myrcene', 'Limonene', 'beta-Caryophyllene', 'Terpinolene',
    'Linalool', 'alpha-Pinene', 'beta-Pinene', 'Caryophyllene Oxide',
    'Guaiol', 'Humulene', 'alpha-Bisabolol', 'Camphene', 'Ocimene'
]

# Map API terpene names (e.g., "BetaCaryophyllene") to our standard names
TERPENE_MAPPING = {
    'betamyrcene': 'beta-Myrcene', 'myrcene': 'beta-Myrcene', 'b-myrcene': 'beta-Myrcene',
    'limonene': 'Limonene', 'd-limonene': 'Limonene',
    'betacaryophyllene': 'beta-Caryophyllene', 'caryophyllene': 'beta-Caryophyllene', 'b-caryophyllene': 'beta-Caryophyllene',
    'terpinolene': 'Terpinolene',
    'linalool': 'Linalool',
    'alphapinene': 'alpha-Pinene', 'a-pinene': 'alpha-Pinene', 'pinene': 'alpha-Pinene',
    'betapinene': 'beta-Pinene', 'b-pinene': 'beta-Pinene',
    'caryophylleneoxide': 'Caryophyllene Oxide',
    'guaiol': 'Guaiol',
    'humulene': 'Humulene', 'alpha-humulene': 'Humulene', 'a-humulene': 'Humulene',
    'alphabisabolol': 'alpha-Bisabolol', 'bisabolol': 'alpha-Bisabolol', 'a-bisabolol': 'alpha-Bisabolol',
    'camphene': 'Camphene',
    'ocimene': 'Ocimene', 'beta-ocimene': 'Ocimene', 'b-ocimene': 'Ocimene'
}

def parse_trulieve_products(products, store_name):
    """
    Parses the 'data' array from the Trulieve API response.
    Each variant of a product becomes a separate row.
    """
    parsed_variants = []
    
    for product in products:
        try:
            common_name = product.get('name', 'N/A')
            brand = product.get('brand', 'N/A')
            category = product.get('category', 'N/A')
            subcategory = product.get('subcategory', 'N/A')
            
            thc = product.get('thc_content')
            cbd = product.get('cbd_content')
            
            terpene_data = {terp: np.nan for terp in KNOWN_TERPENES}
            total_terps = 0
            
            terpenes_list = product.get('terpenes', [])
            if terpenes_list:
                for terp in terpenes_list:
                    name, value = terp.get('name'), terp.get('value')
                    
                    if name and value is not None:
                        clean_name = name.strip().lower().replace('-', '')
                        standard_name = TERPENE_MAPPING.get(clean_name)
                        
                        if standard_name:
                            terpene_data[standard_name], total_terps = value, total_terps + value
                            
            total_terps = total_terps if total_terps > 0 else np.nan

            variants = product.get('variants', [])
            if not variants: continue
                
            for variant in variants:
                weight_str = variant.get('option')
                if not weight_str: continue
                    
                price = variant.get('sale_unit_price') or variant.get('unit_price')
                if not price: continue
                    
                product_row = {
                    'Name': common_name, 'Store': store_name, 'Brand': brand,
                    'Type': category, 'Subtype': subcategory, 'Weight': convert_to_grams(weight_str),
                    'Weight_Str': weight_str, 'Price': float(price),
                    'THC': float(thc) if thc is not None else np.nan,
                    'CBD': float(cbd) if cbd is not None else np.nan,
                    'Total_Terps': total_terps,
                }
                
                product_row.update(terpene_data)
                parsed_variants.append(product_row)

        except Exception as e:
            print(f"Error parsing product: {product.get('name')}. Error: {e}")
            continue
            
    return parsed_variants


def fetch_trulieve_data(stores):
    """
    Main function to orchestrate the Trulieve scraping process.
    """
    all_products_list = []
    print("Starting Trulieve Scraper (api.trulieve.com)...")

    for store_name, store_id in stores.items():
        print(f"Fetching data for Trulieve store: {store_name} (ID: {store_id})...")
        for category in CATEGORIES:
            page = 1
            while True:
                try:
                    url = f"{BASE_URL.format(store_id=store_id, category=category)}?page={page}"
                    response = requests.get(url, headers=HEADERS, timeout=10)
                    response.raise_for_status()
                    
                    json_response = response.json()
                    products = json_response.get('data')
                    
                    if not products:
                        print(f"  ...completed category: {category}")
                        break
                        
                    all_products_list.extend(parse_trulieve_products(products, store_name))
                    
                    last_page, current_page = json_response.get('last_page'), json_response.get('current_page')
                    if last_page is not None and current_page is not None and current_page >= last_page:
                        print(f"  ...completed category: {category}")
                        break
                        
                    page += 1
                    
                except requests.exceptions.RequestException as e:
                    print(f"Error fetching page {page} for {category} at {store_name}: {e}")
                    break
                except Exception as e:
                    print(f"An error occurred processing page {page} for {category}: {e}")
                    break

    if not all_products_list:
        print("No product data was fetched from Trulieve. Returning an empty DataFrame.")
        return pd.DataFrame()

    df = pd.DataFrame(all_products_list)
    df['dpg'] = df['Price'] / df['Weight']
    
    cannabinoid_cols = sorted([col for col in df.columns if col not in KNOWN_TERPENES + ['Name', 'Store', 'Brand', 'Type', 'Subtype', 'Weight', 'Weight_Str', 'Price', 'dpg', 'Total_Terps']])
    column_order = ['Name', 'Store', 'Brand', 'Type', 'Subtype', 'Weight', 'Weight_Str', 'Price', 'dpg', 'Total_Terps'] + cannabinoid_cols + KNOWN_TERPENES
    
    df = df.reindex(columns=column_order)

    print(f"\nScraping complete for Trulieve. DataFrame created with {len(df)} rows.")
    return df
