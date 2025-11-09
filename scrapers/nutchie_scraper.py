# scrapers/dutchie_scraper.py
# This scraper is designed to fetch data from the Dutchie GraphQL API.

import requests
import json
import time

# --- Constants ---
# The DUTCHIE_STORES dictionary is the core configuration for this scraper.
# (This is the dictionary you provided)
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
#   "Liberty (PGH)": {
#       "api_url": "https://dutchie.com/api-1/graphql",
#       "store_id": "63dab2d8ab202100548dbaf5",
#       "headers": {
#           "accept": "*/*",
#           "apollographql-client-name": "Marketplace (production)",
#           "content-type": "application/json",
#           "referer": "https://dutchie.com/embedded-menu/liberty-pittsburgh",
#           "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0",
#           "x-dutchie-session": "eyJpZCI6ImFiMGRlMWMyLWNhNWUtNGM4Zi05NDA1LTRhZWQ5MGNhZTQzOCIsImV4cGlyZXMiOjE3NjI0ODE2MDE3Mjd9"
#       }
#   },
    "Ascend (Cranberry)": {
        "api_url": "https://letsascend.com/api-4/graphql",
        "store_id": "66fef50576b5d1b3703a1890",
        "headers": {
            "accept": "*/*",
            "apollographql-client-name": "Marketplace (production)",
            "content-type": "application/json",
            "referer": "https://letsascend.com/stores/cranberry-pennsylvania",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Version/18.6 Safari/605.1.15",
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
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Version/18.6 Safari/605.1.15",
            "x-dutchie-session": "eyJpZCI6IjNhMTFmZGZhLTU5MGQtNDk5ZC1hYzE4LTRjNjhlZjRjNjZkNiIsImV4cGlyZXMiOjE3NjI0ODA3NzY0ODF9"
        }
    }
}

# --- Main Scraper Function ---

def get_dutchie_data(store_configs):
    """
    Main function to orchestrate the scraping for all configured Dutchie stores.
    """
    all_product_slugs = []
    
    # First, get all product slugs (cName) from all stores
    for store_name, store_config in store_configs.items():
        slugs = get_all_product_slugs(store_name, store_config)
        all_product_slugs.extend(slugs)

    if not all_product_slugs:
        print("No product slugs found for any Dutchie store. Exiting Dutchie scraper.")
        return []

    # Now, get the detailed info for every slug found
    print(f"\nFound {len(all_product_slugs)} total products. Fetching detailed info...")
    product_details = get_detailed_product_info(all_product_slugs)

    if not product_details:
        print("No product data was fetched. Returning an empty list.")
        return []

    return product_details


# --- Step 1: Get Product List ---

def get_all_product_slugs(store_name, store_config):
    """
    Fetches the list of all products for a single store to get their slugs.
    We use the `FilteredProducts` persisted query hash we found earlier.
    """
    print(f"--- Scraping store: {store_name} ---")
    
    api_url = store_config["api_url"]
    headers = store_config["headers"]
    store_id = store_config["store_id"]
    
    all_slugs_for_store = []
    current_page = 0
    products_per_page = 100 # We can use a large limit
    total_products_found = 0

    while True:
        # These are the variables for the `FilteredProducts` query
        variables = {
            "includeEnterpriseSpecials": False,
            "productsFilter": {
                "productIds": [],
                "dispensaryId": store_id,
                "pricingType": "med",
                "strainTypes": [],
                "subcategories": [],
                "Status": "Active",
                "types": [], # Empty means get ALL types
                "useCache": False,
                "isDefaultSort": True,
                "sortBy": "weight",
                "sortDirection": 1,
                "bypassOnlineThresholds": False,
                "isKioskMenu": False,
                "removeProductsBelowOptionThresholds": True
            },
            "page": current_page,
            "perPage": products_per_page
        }
        
        # This is the persisted query hash for `FilteredProducts`
        extensions = {
            "persistedQuery": {
                "version": 1,
                "sha256Hash": "ee29c060826dc41c527e470e9ae502c9b2c169720faa0a9f5d25e1b9a530a4a0"
            }
        }
        
        # Build the URL parameters
        params = {
            'operationName': 'FilteredProducts',
            'variables': json.dumps(variables),
            'extensions': json.dumps(extensions)
        }

        try:
            response = requests.get(api_url, headers=headers, params=params, timeout=20)
            response.raise_for_status()
            data = response.json()

            if "errors" in data:
                print(f"  Error on page {current_page} for {store_name}: {data['errors'][0]['message']}")
                break

            products = data.get("data", {}).get("filteredProducts", {}).get("products", [])
            
            if not products:
                # No products on this page, we're done.
                print(f"  ...completed store. Found {total_products_found} total products.")
                break
                
            print(f"  Page {current_page}: Found {len(products)} products.")
            total_products_found += len(products)
            
            # Extract the 'cName' (slug) and 'retailerId' for each product
            for product in products:
                if product.get('cName') and product.get('DispensaryID'):
                    all_slugs_for_store.append({
                        "store_name": store_name,
                        "cName": product.get('cName'),
                        "retailerId": product.get('DispensaryID'),
                        # We also need the API URL and headers for the *next* request
                        "api_url": api_url,
                        "headers": headers
                    })

            current_page += 1
            time.sleep(0.5) # Politeness delay

        except requests.exceptions.RequestException as e:
            print(f"  Error fetching page {current_page} for {store_name}: {e}")
            break
        except Exception as e:
            print(f"  An error occurred processing page {current_page} for {store_name}: {e}")
            break
            
    return all_slugs_for_store


# --- Step 2: Get Detailed Product Info ---

def get_detailed_product_info(all_slugs):
    """
    Fetches the detailed product info for every product slug collected.
    We use the `IndividualFilteredProduct` persisted query hash.
    
    THIS IS THE MODIFIED STEP. We no longer parse, we just fetch and save the raw JSON.
    """
    product_details_list = []
    
    for i, slug_info in enumerate(all_slugs):
        
        # These are the variables for the `IndividualFilteredProduct` query
        variables = {
            "includeEnterpriseSpecials": False,
            "productsFilter": {
                "cName": slug_info['cName'],
                "dispensaryId": slug_info['retailerId'],
                "removeProductsBelowOptionThresholds": False,
                "isKioskMenu": False,
                "bypassKioskThresholds": False,
                "bypassOnlineThresholds": True,
                "Status": "All"
            }
        }
        
        # This is the persisted query hash for `IndividualFilteredProduct`
        extensions = {
            "persistedQuery": {
                "version": 1,
                "sha256Hash": "47369a02fc8256aaf1ed70d0c958c88514acdf55c5810a5be8e0ee1a19617cda"
            }
        }

        # Build the URL parameters
        params = {
            'operationName': 'IndividualFilteredProduct',
            'variables': json.dumps(variables),
            'extensions': json.dumps(extensions)
        }
        
        print(f"  Fetching detail for product {i+1} of {len(all_slugs)} ({slug_info['cName']})...")

        try:
            response = requests.get(slug_info['api_url'], headers=slug_info['headers'], params=params, timeout=20)
            response.raise_for_status()
            data = response.json()

            if "errors" in data:
                print(f"    Error on product {slug_info['cName']}: {data['errors'][0]['message']}")
                continue # Skip this product

            product_data = data.get("data", {}).get("product", None)
            
            if product_data:
                # Add our own 'store_name' field for easy reference
                product_data['store_name'] = slug_info['store_name']
                # Append the raw product JSON object
                product_details_list.append(product_data)
            else:
                print(f"    No product data found in response for {slug_info['cName']}")

            # Politeness delay
            time.sleep(0.5)

        except requests.exceptions.RequestException as e:
            print(f"  Error fetching detail for {slug_info['cName']}: {e}")
        except Exception as e:
            print(f"  An error occurred processing {slug_info['cName']}: {e}")

    return product_details_list