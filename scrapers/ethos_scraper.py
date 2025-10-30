# scrapers/ethos_scraper.py
# This scraper fetches data from the Dutchie GraphQL API via the Ethos proxy.
# This file REPLACES the old dutchie_scraper.py

import requests
import pandas as pd
import numpy as np
import json # Import json for stringifying
from .scraper_utils import convert_to_grams

# --- Constants ---
# This is the Ethos proxy URL
API_URL = "https://harmarville.ethoscannabis.com/api-4/graphql"

# Full headers from browser inspection
HEADERS = {
    "accept": "*/*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-US,en;q=0.9,it-IT;q=0.8,it;q=0.7",
    "apollographql-client-name": "Marketplace (production)",
    "content-type": "application/json",
    "cookie": "_gcl_au=1.1.863854159.1761849312; __qca=P1-86903132-3660-45ef-91a9-23bc9a571cb6; surfses.04fa=*; surfid.04fa=3e467bfd-f0b7-44cf-934b-bc3e122ce283.1761849314.1.1761849314.1761849314.ec845482-9969-4238-9129-537bb0456306; dsid=c833f52b-a80a-4154-92bc-9085c9d50ea5; __ssid=d0ae3ebce9738b52ad22a167a916c02; _ga=GA1.3.337622672.1761849312; _gid=GA1.3.1450677294.1761849321; sa-user-id=s%253A0-c35cf10c-424c-5ef6-71e1-b3ba204957be.nXIM%252BVbLSuiHj6rNxUQdPABaJ3fVpwLSE3Jd3s8h2Jw; sa-user-id-v2=s%253Aw1zxDEJMXvZx4bO6IElXvoiOGb4.XT4pdWc%252BOLpze2LG0KH5CezJMwU9yfykh5QznHWN1Dc; userId=7a3llacg1duoftgth6r8i46696; sessionId=bb2ow1s44xwln9mpp9rhdm; __kla_id=eyJjaWQiOiJOamczTkRjMVpqSXRPR00zWkMwME9EaGlMV0ZpTVRRdE9EUTNNR1psTnpabFpqY3gifQ==; _fbp=fb.1.1761849321927.515571872106850424; _clck=vrzoaa%5E2%5Eg0l%5E0%5E2129; rl_visitor_history=6ce31ee6-a549-4c7b-b9f8-c2f37e6e874f; sifi_user_id=undefined; _sp_ses.04fa=*; osid=ac0c7e5c-3533-4c4c-9a2f-f256e2745a95; sa-user-id-v3=s%253AAQAKIB7kICeH-2IorkruYh7iVPm_L8lH23PUsv9G18cSQr1aEFsYBCDz347IBjABOgQrFVvoQgSah42G.FFx6sPFIBdENg4JHqJOJoMkXPHx3P2RrMUHl9VkuHPE; _uetsid=314693b0b5bf11f0871a792eb15849bd; _uetvid=3146adf0b5bf11f0aff5533762e94a55; _gat=1; _gat_dispensaryTracker=1; _clsk=7fipf4%5E1761850687770%5E5%5E1%5Ei.clarity.ms%2Fcollect; _sp_id.04fa=b9c10b1a-21d2-4453-b6a7-b394fe8561ec.1761849323.1.1761851067.1761849323.6d650209-e1a2-4c6b-9db6-83c599ea837d; __cf_bm=ujDPmAIp6q9D1jgbk_4UUyMp9xGxco5Ae3BVbl4rV.0-1761851074-1.0.1.1-yqtj2.DtEH8LMvInECxwUK91SzoMes7adPXj_.mkpPSdGSLqCiEn5Bu.Imi0V3.MjNu8.X1cRV0sEawuu.Th6DMdQXEmT09NSX3S9_V3zCs; _ga_FZN7LD29Z4=GS2.1.s1761849321$o1$g1$t1761851069$j60$l0$h0; _ga_LJ036PP29V=GS2.1.s1761849312$o1$g1$t1761851069$j60$l0$h0; _dd_s=logs=1&id=0a41edd1-c84c-4fd4-a9eb-2ec27ff2c73c&created=1761849319788&expire=1761851970252; _lr_hb_-zg2tcu%2Fdutchie-v2={%22heartbeat%22:1761851070311}; _lr_tabs_-zg2tcu%2Fdutchie-v2={%22recordingID%22:%226-019a3681-d74e-7858-a89b-55674eb4654f%22,%22sessionID%22:0,%22lastActivity%22:1761851070543,%22hasActivity%22:false,%22clearsIdentifiedUser%22:false,%22confirmed%22:true}; _ga_ZNEV4RY2MG=GS2.1.s1761849321$o1$g1$t1761851070$j59$l0$h0; _ga=GA1.2.337622672.1761849312",
    "priority": "u=1, i",
    "referer": "https://harmarville.ethoscannabis.com/stores/ethos-harmarville/products/flower",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "url": "https://harmarville.ethoscannabis.com/stores/ethos-harmarville/products/flower",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0",
    "x-dutchie-session": "eyJpZCI6Ijk0YTNhOWU4LTZkNzctNDY2OC04MzFjLTA2YmFlOWU5ODdmYSIsImV4cGlyZXMiOjE3NjI0NTc3MTk4Mjl9"
}

# Add all discovered Ethos store IDs
STORES = {
    "Ethos (Harmar)": "621900cebbc5580e15476deb",
    "Ethos (Pleasant Hills)": "607dc27bfde18500b5e8dd52",
    "Ethos (North Fayette)": "5fa0829005bb2400cfc4b694"
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

def get_all_product_slugs():
    """
    Step 1: Fetch all product cNames (slugs) for all stores and categories.
    """
    all_products = []
    print("Step 1: Fetching all product slugs from Ethos (Dutchie Proxy)...")

    for store_name, store_id in STORES.items():
        # Update the referer header for the specific store
        headers = HEADERS.copy()
        headers['referer'] = f"https://harmarville.ethoscannabis.com/stores/{store_name.lower().replace(' ', '-').replace('(', '').replace(')', '')}"
        
        for category in CATEGORIES:
            page = 0
            while True:
                # These are the variables for the product list query
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
                    "perPage": 100 # Increased perPage
                }
                
                # This is the hash for the "FilteredProducts" operation
                extensions = {
                    "persistedQuery": {
                        "version": 1,
                        "sha256Hash": "f3b8a3820696e2a6d06487ebb4d86df454a90ed863185c1a659e7a126f391644"
                    }
                }
                
                # This request is a GET, so parameters are sent in the URL
                params = {
                    'operationName': 'FilteredProducts',
                    'variables': json.dumps(variables),
                    'extensions': json.dumps(extensions)
                }
                
                try:
                    response = requests.get(API_URL, headers=headers, params=params)
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
                            "StoreName": store_name
                        })
                    page += 1
                except requests.exceptions.RequestException as e:
                    print(f"Error fetching product slugs for {store_name} ({category}): {e}")
                    break  # Stop trying for this store/category on error
                except KeyError:
                    print(f"Unexpected JSON structure for {store_name} ({category}). Skipping.")
                    print(f"Response was: {response.text}")
                    break

    print(f"  ...found {len(all_products)} total product slugs.")
    return all_products

def get_detailed_product_info(product_slugs):
    """
    Step 2 & 3: Fetch detailed info for each product and parse the data.
    """
    all_product_data = []
    print("\nStep 2: Fetching detailed product information from Ethos...")

    for i, slug_info in enumerate(product_slugs):
        cName = slug_info['cName']
        dispensaryId = slug_info['DispensaryID']
        store_name = slug_info['StoreName']

        if (i + 1) % 50 == 0:
            print(f"  ...processing product {i + 1}/{len(product_slugs)}")

        # These are the variables for the *individual* product query
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
        
        # This is the hash for the "IndividualFilteredProduct" operation
        extensions = {
            "persistedQuery": {
                "version": 1,
                "sha256Hash": "7e321b76b73d96861462a1c4f676cab46e7a0745f6ac63538498d51f0aae1507"
            }
        }
        
        # This request is also a GET
        params = {
            'operationName': 'IndividualFilteredProduct',
            'variables': json.dumps(variables),
            'extensions': json.dumps(extensions)
        }
        
        # Update the referer header for the specific store
        headers = HEADERS.copy()
        headers['referer'] = f"https://harmarville.ethoscannabis.com/stores/{store_name.lower().replace(' ', '-').replace('(', '').replace(')', '')}"

        try:
            response = requests.get(API_URL, headers=headers, params=params)
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
    
    # Use special price if available, otherwise regular price
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
            # Assuming 'name' and 'value' are the correct keys
            if 'name' in cannabinoid and 'value' in cannabinoid:
                cannabinoid_data[cannabinoid['name']] = cannabinoid['value']

    # Fallback for THC/CBD
    thc_range = product.get('THCContent', {}).get('range', [])
    cbd_range = product.get('CBDContent', {}).get('range', [])
    
    # Use the first value in the range if it exists
    if 'THC' not in cannabinoid_data and thc_range and thc_range[0] is not None:
        cannabinoid_data['THC'] = thc_range[0]
    if 'CBD' not in cannabinoid_data and cbd_range and cbd_range[0] is not None:
        cannabinoid_data['CBD'] = cbd_range[0]
        
    data.update(cannabinoid_data)

    # --- Terpenes ---
    terpene_data = {terp: np.nan for terp in KNOWN_TERPENES} # Default to NaN
    total_terps = 0
    if product.get('terpenes'): 
        for terp in product['terpenes']:
            name = terp.get('name', terp.get('libraryTerpene', {}).get('name'))
            value = terp.get('value')
            if name and value is not None:
                # Basic standardization
                clean_name = name.strip().lower().replace('-', '')
                standard_name = TERPENE_MAPPING.get(clean_name)
                
                if standard_name:
                    terpene_data[standard_name] = value
                    total_terps += value

    data.update(terpene_data)
    data['Total_Terps'] = total_terps if total_terps > 0 else np.nan

    return data

def fetch_ethos_data():
    """
    Main function to orchestrate the Ethos scraping process.
    """
    # Step 1: Get all product slugs
    product_slugs = get_all_product_slugs()

    if not product_slugs:
        print("No product slugs found for Ethos. Exiting Ethos scraper.")
        return pd.DataFrame()

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
    cannabinoid_cols = sorted([col for col in df.columns if col not in KNOWN_TERPENES + ['Name', 'Store', 'Brand', 'Type', 'Subtype', 'Weight', 'Weight_Str', 'Price', 'dpg', 'Total_Terps']])
    terpene_cols = KNOWN_TERPENES

    column_order = (
        ['Name', 'Store', 'Brand', 'Type', 'Subtype', 'Weight', 'Weight_Str', 'Price', 'dpg', 'Total_Terps'] +
        cannabinoid_cols +
        terpene_cols
    )

    # Reorder and fill NaNs
    df = df.reindex(columns=column_order).fillna(np.nan)

    print("\nScraping complete for Ethos. DataFrame created.")
    return df
