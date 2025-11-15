import cloudscraper
import json
import requests

# 1. Use cloudscraper to create a session object.
# This object will automatically handle the Cloudflare JavaScript challenge.
scraper = cloudscraper.create_scraper() 

# 2. The base URL
api_url = "https://dutchie.com/api-2/graphql"

# 3. The Request Headers
# We let cloudscraper handle the tricky cookies,
# but we still need to provide the other headers.
headers = {
    'accept': '*/*',
    'accept-encoding': 'gzip, deflate, br, zstd',
    'accept-language': 'en-US,en;q=0.9,it-IT;q=0.8,it;q=0.7',
    'apollographql-client-name': 'Marketplace (production)',
    'content-type': 'application/json',
    'priority': 'u=1, i',
    'referer': 'https.dutchie.com/embedded-menu/ayr-dispensary-gibsonia/products/flower?',
    'sec-ch-ua': '"Chromium";v="142", "Microsoft Edge";v="142", "Not_A Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-storage-access': 'active',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0',
    
    # We provide the application session, but NOT the Cloudflare cookie.
    'x-dutchie-session': 'eyJpZCI6IjIxNjRkNWEwLTA1ODktNDUyZS1iNmVhLTY2NzhlMDc2MzllZCIsImV4cGlyZXMiOjE3NjM3MzgxMzc0MTV9'
    
    # NO 'cookie' HEADER. Let cloudscraper handle it.
}

# 4. The Query Parameters (unchanged)
variables_dict = {
    "includeEnterpriseSpecials": False,
    "productsFilter": {
        "productIds": [],
        "dispensaryId": "5ff8ee358174a300e11a15cb",
        "pricingType": "med",
        "strainTypes": [],
        "subcategories": [],
        "Status": "Active",
        "types": ["Flower"],
        "useCache": False,
        "isDefaultSort": True,
        "sortBy": "alpha",
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

# 5. Make the request using scraper.get()
try:
    print(f"Attempting to GET: {api_url}")
    # Use scraper.get() instead of requests.get()
    response = scraper.get(api_url, headers=headers, params=params) 
    response.raise_for_status()
    data = response.json()

    # --- Now you can work with the data ---
    product_list = data.get('data', {}).get('filteredProducts', {}).get('products', [])
    
    if product_list:
        print(f"\nSuccess! Found {len(product_list)} products.")
        
        for product in product_list[:5]:
            product_name = product.get('name', 'N/A')
            brand_name = product.get('brand', {}).get('name', 'N/A')
            
            print(f"\n- Product: {product_name}")
            print(f"  Brand: {brand_name.strip()}")

    else:
        print("\nCall succeeded but no products were found in the response.")

except requests.exceptions.HTTPError as errh:
    print(f"\nHttp Error: {errh}")
    print(f"Response content: {response.text}")
except json.JSONDecodeError:
    print(f"\nFailed to decode JSON. Status code: {response.status_code}")
    print(f"Response content: {response.text}")
except requests.exceptions.RequestException as err:
    print(f"\nAn Error Occurred: {err}")
