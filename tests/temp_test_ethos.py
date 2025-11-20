import requests
import json

# 1. The base URL (without the query string)
api_url = "https://nephilly.ethoscannabis.com/api-4/graphql"

# 2. The Request Headers
# These are copied from your browser's request
headers = {
    'accept': '*/*',
    'accept-encoding': 'gzip, deflate, br, zstd',
    'accept-language': 'en-US,en;q=0.9,it-IT;q=0.8,it;q=0.7',
    'apollographql-client-name': 'Marketplace (production)',
    'content-type': 'application/json',
    'priority': 'u=1, i',
    'referer': 'https://letsascend.com/stores/cranberry-pennsylvania',
    'sec-ch-ua': '"Chromium";v="142", "Microsoft Edge";v="142", "Not_A Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0',
    'x-dutchie-session': 'eyJpZCI6Ijc4OTJjNjhiLWExMmEtNGVlZS04NTQ5LTg3MGFjMDhkMzlhMiIsImV4cGlyZXMiOjE3NjM3NjA5ODYzMjh9'
}

# 3. The Query Parameters
# Build the 'variables' and 'extensions' objects as Python dicts
variables_dict = {
    "includeEnterpriseSpecials": False,
    "productsFilter": {
        "dispensaryId": "5f2de49198211000abef8b99",
        "pricingType": "med",
        "strainTypes": [],
        "subcategories": [],
        "Status": "Active",
        "types": [],
        "useCache": False,
        "isDefaultSort": True,
        "sortBy": "popularSortIdx",
        "sortDirection": 1,
        "bypassOnlineThresholds": False,
        "isKioskMenu": False,
        "removeProductsBelowOptionThresholds": True
    },
    "page": 0,
    "perPage": 50
}

extensions_dict = {
    "persistedQuery": {
        "version": 1,
        "sha256Hash": "ee29c060826dc41c527e470e9ae502c9b2c169720faa0a9f5d25e1b9a530a4a0"
    }
}

# Create the final params dict for the request.
# We use json.dumps() to convert the Python dicts into JSON strings,
# which is what the API is expecting.
params = {
    'operationName': 'FilteredProducts',
    'variables': json.dumps(variables_dict),
    'extensions': json.dumps(extensions_dict)
}

# 4. Make the request
try:
    print(f"Attempting to GET: {api_url}")
    
    # The requests library will automatically URL-encode the 'params'
    response = requests.get(api_url, headers=headers, params=params)
    
    # This will raise an HTTPError if the response status is 4xx or 5xx
    response.raise_for_status()
    
    print("\nSuccess! Response (first 500 chars):")
    # Assuming the response is JSON, as indicated by 'content-type'
    data = response.json()
    print(json.dumps(data, indent=2)[:500] + "...")

except requests.exceptions.HTTPError as errh:
    print(f"\nHttp Error: {errh}")
    print(f"Response content: {response.text}")
except requests.exceptions.ConnectionError as errc:
    print(f"\nError Connecting: {errc}")
except requests.exceptions.Timeout as errt:
    print(f"\nTimeout Error: {errt}")
except requests.exceptions.RequestException as err:
    print(f"\nAn Error Occurred: {err}")