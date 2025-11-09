import requests
import json
from scrapers.dutchie_scraper import DUTCHIE_STORES

# Select a store
store_name = "Ethos (Harmar)"
store_config = DUTCHIE_STORES[store_name]

# Extract store details
api_url = store_config["api_url"]
# Start with the headers from your config
headers = store_config["headers"]
store_id = store_config["store_id"]

# --- Data from your Network Log ---

# 1. The operationName
operation_name = "FilteredProducts"

# 2. The variables, structured as a Python dict.
# We're using the store_id from your config file.
# We've set "types": [] to get all products, not just "Vaporizers".
variables = {
    "includeEnterpriseSpecials": False,
    "productsFilter": {
        "productIds": [],
        "dispensaryId": store_id,
        "pricingType": "med",
        "strainTypes": [],
        "subcategories": [],
        "Status": "Active",
        "types": [],  # Empty list to get ALL product types
        "useCache": False,
        "isDefaultSort": True,
        "sortBy": "weight",
        "sortDirection": 1,
        "bypassOnlineThresholds": False,
        "isKioskMenu": False,
        "removeProductsBelowOptionThresholds": True
    },
    "page": 0,
    "perPage": 25  # Let's get 25 items to start
}

# 3. The 'extensions' block with the persisted query hash
extensions = {
    "persistedQuery": {
        "version": 1,
        "sha256Hash": "ee29c060826dc41c527e470e9ae502c9b2c169720faa0a9f5d25e1b9a530a4a0"
    }
}

# 4. Add the 'apollographql-client-name' header from your log
headers['apollographql-client-name'] = 'Marketplace (production)'

# 5. Build the URL parameters.
# We must convert the variables and extensions dicts into JSON strings.
params = {
    'operationName': operation_name,
    'variables': json.dumps(variables),
    'extensions': json.dumps(extensions)
}

print(f"Sending GET request with Persisted Query to: {api_url}")

# --- Make the GET Request ---
# We use 'params=' to send these as URL query parameters
response = requests.get(api_url, headers=headers, params=params)

# Print the response
print("Status Code:", response.status_code)

try:
    response_json = response.json()
    
    # Write the full JSON response to a file
    output_filename = "query_results.json"
    with open(output_filename, "w", encoding="utf-8") as f:
        json.dump(response_json, f, indent=2, ensure_ascii=False)
    
    print(f"\nSuccessfully saved full JSON response to {output_filename}")

    if "errors" in response_json:
        print("\n--- GraphQL Errors ---")
        for error in response_json["errors"]:
            print(error["message"])
        print("----------------------")
    elif "data" in response_json:
        print("\n--- Query Successful! ---")
        print(f"Data saved to {output_filename}. Review the file to see the data structure.")

except json.JSONDecodeError:
    print(f"\nFailed to decode JSON. Raw response:\n{response.text}")