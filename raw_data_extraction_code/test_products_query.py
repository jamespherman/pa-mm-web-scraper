import requests
import json
from scrapers.dutchie_scraper import DUTCHIE_STORES

# Select a store
store_name = "Ethos (Harmar)"
store_config = DUTCHIE_STORES[store_name]

# Extract store details
api_url = store_config["api_url"]
headers = store_config["headers"]
store_id = store_config["store_id"]

# Construct the new GraphQL query based on common patterns
# 1. Renamed 'menu' to 'products' (a more common query name)
# 2. Replaced 'pagination: Pagination!' with 'limit: Int, offset: Int'
# 3. Assumed 'products' returns an object with 'items' (the list) and 'total' (the count)
query = """
query GetProducts($retailerId: ID!, $limit: Int, $offset: Int) {
  products(retailerId: $retailerId, limit: $limit, offset: $offset) {
    items {
      id
      name
      brand {
        name
      }
      category {
        name
      }
      terpenes {
        name
        value
      }
      cannabinoids {
        name
        value
      }
    }
    total
  }
}
"""

# Update variables:
# 1. Removed the nested 'pagination' object
# 2. Promoted 'limit' and 'offset' to the top level
variables = {
    "retailerId": store_id,
    "limit": 5,
    "offset": 0
}

print(f"Sending new 'products' query to: {api_url}")

# Send the POST request
response = requests.post(api_url, headers=headers, json={"query": query, "variables": variables})

# Print the response
print("Status Code:", response.status_code)
response_json = response.json()
print(json.dumps(response_json, indent=2))

# Check for and print GraphQL errors
if "errors" in response_json:
    print("\n--- GraphQL Errors ---")
    for error in response_json["errors"]:
        print(error["message"])
    print("----------------------")
elif "data" in response_json:
    print("\n--- Query Successful! ---")
    print("This query structure appears to be correct.")