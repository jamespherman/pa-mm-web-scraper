import requests
import json
import time  # Import the time module for delays
from scrapers.dutchie_scraper import DUTCHIE_STORES

# Select a store
store_name = "Ethos (Harmar)"
store_config = DUTCHIE_STORES[store_name]

# Extract store details
api_url = store_config["api_url"]
headers = store_config["headers"]
store_id = store_config["store_id"]

# --- Base data from your Network Log ---
operation_name = "FilteredProducts"
extensions = {
    "persistedQuery": {
        "version": 1,
        "sha256Hash": "ee29c060826dc41c527e470e9ae502c9b2c169720faa0a9f5d25e1b9a530a4a0"
    }
}
base_variables = {
    "includeEnterpriseSpecials": False,
    "productsFilter": {
        "productIds": [],
        "dispensaryId": store_id,
        "pricingType": "med",
        "strainTypes": [],
        "subcategories": [],
        "Status": "Active",
        "types": [],
        "useCache": False,
        "isDefaultSort": True,
        "sortBy": "weight",
        "sortDirection": 1,
        "bypassOnlineThresholds": False,
        "isKioskMenu": False,
        "removeProductsBelowOptionThresholds": True
    },
    "perPage": 25  # We know this works
}

# Add the 'apollographql-client-name' header from your log
headers['apollographql-client-name'] = 'Marketplace (production)'

# --- Pagination Logic ---
all_products = []
total_pages = 1  # Start with 1, we'll update this after the first request
current_page = 0

print(f"Starting scraper for: {api_url}")

try:
    while current_page < total_pages:
        print(f"Fetching page {current_page + 1}/{total_pages}...")

        # Set the current page in the variables
        base_variables['page'] = current_page

        # Build the URL parameters for this page
        params = {
            'operationName': operation_name,
            'variables': json.dumps(base_variables),
            'extensions': json.dumps(extensions)
        }

        # --- Make the GET Request ---
        response = requests.get(api_url, headers=headers, params=params)

        if response.status_code != 200:
            print(f"Error: Received status code {response.status_code} on page {current_page}")
            # Optionally, break or continue
            break
        
        response_json = response.json()

        if "errors" in response_json:
            print(f"GraphQL Error on page {current_page}: {response_json['errors']}")
            break

        if "data" not in response_json or "filteredProducts" not in response_json["data"]:
            print(f"Unexpected data structure on page {current_page}. Halting.")
            break
            
        # --- Process Data ---
        products_data = response_json["data"]["filteredProducts"]
        
        # Add products from this page to our master list
        if "products" in products_data and products_data["products"]:
            all_products.extend(products_data["products"])

        # On the first iteration (page 0), set the total_pages
        if current_page == 0:
            total_pages = products_data.get("queryInfo", {}).get("totalPages", 1)
            print(f"Discovered {total_pages} total pages ({products_data.get('queryInfo', {}).get('totalCount', 0)} items).")

        # Increment page counter and sleep to be polite to the server
        current_page += 1
        time.sleep(0.5)  # 0.5 second delay between requests

    print(f"\n--- Scraping Complete ---")
    print(f"Successfully fetched {len(all_products)} products from {total_pages} pages.")

    # Write the full list of products to a file
    output_filename = "all_products.json"
    with open(output_filename, "w", encoding="utf-8") as f:
        json.dump(all_products, f, indent=2, ensure_ascii=False)
    
    print(f"Successfully saved all products to {output_filename}")

except requests.exceptions.RequestException as e:
    print(f"\nA critical error occurred during the request: {e}")
except json.JSONDecodeError:
    print(f"\nFailed to decode JSON. Raw response:\n{response.text}")