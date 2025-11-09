import requests
import json
import time
from scrapers.dutchie_scraper import DUTCHIE_STORES

# --- Setup ---
# Load the store config just like the other scripts
store_name = "Ethos (Harmar)"
store_config = DUTCHIE_STORES[store_name]
api_url = store_config["api_url"]
headers = store_config["headers"]
store_id = store_config["store_id"]

# Add the client name header from the network log
headers['apollographql-client-name'] = 'Marketplace (production)'

# --- Config for the Individual Product Query (from your log) ---
operation_name = "IndividualFilteredProduct"
extensions = {
    "persistedQuery": {
        "version": 1,
        "sha256Hash": "47369a02fc8256aaf1ed70d0c958c88514acdf55c5810a5be8e0ee1a19617cda"
    }
}
# Base variables for the new query
base_variables = {
    "includeEnterpriseSpecials": False,
    "productsFilter": {
        "cName": "",  # This will be filled in by the loop
        "dispensaryId": store_id,
        "removeProductsBelowOptionThresholds": False,
        "isKioskMenu": False,
        "bypassKioskThresholds": False,
        "bypassOnlineThresholds": True,
        "Status": "All"
    }
}

# --- 1. Load the list of products we already scraped ---
def load_product_list(filename="all_products.json"):
    """Loads the product cName list from our first scrape."""
    try:
        with open(filename, "r", encoding="utf-8") as f:
            products = json.load(f)
        
        # We only need the 'cName' from each product
        cnames = [p.get("cName") for p in products if p.get("cName")]
        print(f"Loaded {len(cnames)} product cNames from {filename}.")
        return cnames
    except FileNotFoundError:
        print(f"Error: {filename} not found.")
        print("Please run 'scrape_all_products.py' first to generate it.")
        return []
    except Exception as e:
        print(f"An error occurred loading {filename}: {e}")
        return []

# --- 2. Loop and Fetch Details ---
def fetch_all_product_details(cnames):
    """
    Loops through each cName and fetches its detailed product page.
    """
    all_detailed_products = []
    total_products = len(cnames)
    
    print(f"Starting to fetch details for {total_products} products...")

    for i, cname in enumerate(cnames):
        print(f"Fetching product {i+1}/{total_products}: {cname}")
        
        # Set the cName for this specific request
        base_variables["productsFilter"]["cName"] = cname

        # Build the final request parameters
        params = {
            'operationName': operation_name,
            'variables': json.dumps(base_variables),
            'extensions': json.dumps(extensions)
        }
        
        try:
            # --- Make the GET Request ---
            response = requests.get(api_url, headers=headers, params=params)

            if response.status_code == 200:
                response_json = response.json()
                if "data" in response_json and "filteredProducts" in response_json["data"]:
                    # The result is nested, so we extract it
                    product_details = response_json["data"]["filteredProducts"]
                    all_detailed_products.append(product_details)
                elif "errors" in response_json:
                    print(f"  GraphQL Error for {cname}: {response_json['errors'][0]['message']}")
            else:
                print(f"  HTTP Error for {cname}: Status Code {response.status_code}")

            # Be a polite scraper! Wait half a second between requests.
            time.sleep(0.5)

        except requests.exceptions.RequestException as e:
            print(f"  Request Error for {cname}: {e}")
            time.sleep(5) # Wait longer if there's a connection error
        except json.JSONDecodeError:
            print(f"  JSON Decode Error for {cname}. Raw response:\n{response.text[:100]}...")

    return all_detailed_products

# --- Main Execution ---
if __name__ == "__main__":
    product_cnames = load_product_list()
    
    if product_cnames:
        detailed_products = fetch_all_product_details(product_cnames)
        
        print(f"\n--- Scraping Complete ---")
        print(f"Successfully fetched details for {len(detailed_products)} products.")
        
        # Write the new, highly-detailed list to a file
        output_filename = "all_products_detailed.json"
        with open(output_filename, "w", encoding="utf-8") as f:
            json.dump(detailed_products, f, indent=2, ensure_ascii=False)
        
        print(f"Successfully saved all detailed product info to {output_filename}")
        print("\nReview this new file - it should contain the terpene data!")