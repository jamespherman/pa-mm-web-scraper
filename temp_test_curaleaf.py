import requests
import json

# 1. The base URL
api_url = "https://curaleaf.com/api-2/graphql"

# 2. The Request Headers
# We are ADDING THE COOKIE BACK IN, as this is the most likely fix
headers = {
    'accept': '*/*',
    'accept-encoding': 'gzip, deflate, br, zstd',
    'accept-language': 'en-US,en;q=0.9,it-IT;q=0.8,it;q=0.7',
    'apollographql-client-name': 'Marketplace (production)',
    'content-type': 'application/json',

    # Find the 'dsid' value from your original string
    'cookie': 'confirmed21OrOlder=1',

    'priority': 'u=1, i',
    'referer': 'https://curaleaf.com/stores/curaleaf-pa-gettysburg/products/flower',
    'sec-ch-prefers-color-scheme': 'light',
    'sec-ch-ua': '"Chromium";v="142", "Microsoft Edge";v="142", "Not_A Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0',
    'x-dutchie-session': 'eyJpZCI6IjE5NTA1MGVmLTQ2MzMtNGRhYS05YjA5LTc4MzQ1ZDU0MTlhMSIsImV4cGlyZXMiOjE3NjM0ODcxMDExMTd9'
}

# 3. The Query Parameters (unchanged)
variables_dict = {
    "includeEnterpriseSpecials": False,
    "productsFilter": {
        "productIds": [],
        "dispensaryId": "6074c37fcee012009f173ff2",
        "pricingType": "med",
        "strainTypes": [],
        "subcategories": [],
        "Status": "Active",
        "types": ["Flower"],
        "useCache": False,
        "isDefaultSort": True,
        "sortBy": "default",
        "sortDirection": 1,
        "bypassOnlineThresholds": False,
        "isKioskMenu": False,
        "removeProductsBelowOptionThresholds": True
    },
    "page": 0,
    "perPage": 100
}

extensions_dict = {
    "persistedQuery": {
        "version": 1,
        "sha256Hash": "ee29c060826dc41c527e470e9ae502c9b2c169720faa0a9f5d25e1b9a530a4a0"
    }
}

params = {
    'operationName': 'FilteredProducts',
    'variables': json.dumps(variables_dict),
    'extensions': json.dumps(extensions_dict)
}

# 4. Make the request
try:
    print(f"Attempting to GET: {api_url}")
    
    response = requests.get(api_url, headers=headers, params=params)
    
    print(f"Status Code: {response.status_code}")
    
    # This will raise an HTTPError if the response status is 4xx or 5xx
    response.raise_for_status()
    
    # *** DEBUGGING STEP ***
    # Print the raw text BEFORE trying to parse it
    print("\n--- Raw Response Text (first 500 chars) ---")
    print(response.text[:500])
    print("------------------------------------------\n")

    # Now, try to parse it
    print("Attempting to parse JSON...")
    data = response.json()
    
    print("Success! Response (first 500 chars of JSON):")
    print(json.dumps(data, indent=2)[:500] + "...")

except requests.exceptions.HTTPError as errh:
    print(f"\nHttp Error: {errh}")
    print(f"Response content: {response.text}")
except json.JSONDecodeError:
    # Catch the specific error you saw
    print("\n--- FAILED TO DECODE JSON ---")
    print("The response was not valid JSON. The server likely returned an HTML page or empty response.")
    print("Review the 'Raw Response Text' above to see what the server sent.")
except requests.exceptions.RequestException as err:
    print(f"\nAn Error Occurred: {err}")