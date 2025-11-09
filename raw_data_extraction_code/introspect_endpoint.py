import requests
import json

# This script assumes you have the 'scrapers.dutchie_scraper' module
# in your project, just like your original script.
try:
    from scrapers.dutchie_scraper import DUTCHIE_STORES
except ImportError:
    print("Error: Could not import DUTCHIE_STORES from scrapers.dutchie_scraper.")
    print("Please make sure that file exists and is accessible.")
    # As a fallback, create a placeholder so the script can be reviewed
    # The user will need to ensure their import works to run it.
    DUTCHIE_STORES = {
        "Ethos (Harmar)": {
            "api_url": "YOUR_API_URL_HERE",
            "headers": {"Content-Type": "application/json"},
            "store_id": "YOUR_STORE_ID_HERE"
        }
    }

# Select the same store to get the correct api_url and headers
store_name = "Ethos (Harmar)"

if store_name not in DUTCHIE_STORES:
    print(f"Error: Store '{store_name}' not found in DUTCHIE_STORES.")
    exit()

store_config = DUTCHIE_STORES[store_name]

# Extract store details
api_url = store_config["api_url"]
headers = store_config["headers"]

# This is a standard GraphQL introspection query.
# It asks the server to describe itself:
# 1. __schema: The entry point for all introspection.
# 2. queryType: Asks for the name of the root query type (usually "Query").
# 3. fields: Asks for a list of all available queries.
# 4. name/description: We ask for the name and description of each query.
# 5. mutationType: We do the same for all available mutations.
query = """
  query IntrospectionQuery {
    __schema {
      queryType {
        name
        fields {
          name
          description
        }
      }
      mutationType {
        name
        fields {
          name
          description
        }
      }
    }
  }
"""

# Introspection queries do not require variables
variables = {}

print(f"Sending introspection query to: {api_url}")

try:
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
        print("\nThis likely means introspection is disabled or the query is malformed.")
    elif "data" in response_json:
        print("\n--- Introspection Successful! ---")
        print("The server supports introspection. Review the JSON output above.")
        print("Look for the 'queryType' fields to find the correct query name (it might be 'menuByRetailer' or something similar).")

except requests.exceptions.RequestException as e:
    print(f"\nAn error occurred during the request: {e}")
except json.JSONDecodeError:
    print(f"\nFailed to decode JSON response. Raw response text:\n{response.text}")