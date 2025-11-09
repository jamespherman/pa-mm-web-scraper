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

# Construct the GraphQL menu query
query = """
query Menu($retailerId: ID!, $pagination: Pagination!) {
  menu(retailerId: $retailerId, pagination: $pagination) {
    products {
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
  }
}
"""

variables = {
    "retailerId": store_id,
    "pagination": {
        "limit": 5,
        "offset": 0
    }
}

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
