import requests
import json
import pandas as pd

# --- CONFIGURATION ---
# Store ID 86 = Trulieve (Pittsburgh Squirrel Hill)
STORE_ID = "86"
BASE_URL = f"https://api.trulieve.com/api/v2/menu/{STORE_ID}/all/RECREATIONAL"

# Headers copied from your browser inspection (crucial for bypassing WAF)
HEADERS = {
    "accept": "*/*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-US,en;q=0.9,it-IT;q=0.8,it;q=0.7",
    "origin": "https://www.trulieve.com",
    "referer": "https://www.trulieve.com/",
    "sec-ch-ua": '"Chromium";v="142", "Microsoft Edge";v="142", "Not_A Brand";v="99"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0"
}

def test_trulieve_fetch():
    print(f"Testing Trulieve API for Store ID: {STORE_ID}...")
    print(f"Target URL: {BASE_URL}")
    
    # Add pagination parameter
    params = {
        "page": 1,
        "sort_by": "default"
    }

    try:
        response = requests.get(BASE_URL, headers=HEADERS, params=params, timeout=10)
        
        print(f"\nResponse Status Code: {response.status_code}")
        
        if response.status_code != 200:
            print("Error: API request failed.")
            print(response.text[:500]) # Print first 500 chars of error
            return

        data = response.json()
        
        # Extract products
        products = data.get('data', [])
        meta = data.get('meta', {}) # Usually contains pagination info
        
        print(f"\nSuccess! Found {len(products)} products on Page 1.")
        if meta:
            print(f"Pagination Info: {meta}")

        if products:
            print("\n--- First 3 Products Found ---")
            for i, p in enumerate(products[:3]):
                print(f"\n[Product {i+1}]")
                print(f"  Name: {p.get('name')}")
                print(f"  Brand: {p.get('brand')}")
                print(f"  Category: {p.get('category')}")
                
                # Check for nested variants (prices/weights)
                variants = p.get('variants', [])
                if variants:
                    first_variant = variants[0]
                    print(f"  Price: ${first_variant.get('price') or first_variant.get('special_price')}")
                    print(f"  Weight: {first_variant.get('weight')}")
                
                # Check for Terpenes (Critical for your project)
                terpenes = p.get('terpenes', [])
                if terpenes:
                    print(f"  Terpenes Found: {len(terpenes)} (e.g. {terpenes[0].get('name')}: {terpenes[0].get('value')}%)")
                else:
                    print("  WARNING: No terpenes found in this object.")

        else:
            print("Warning: Request succeeded but returned 0 products.")

    except Exception as e:
        print(f"\nCRITICAL ERROR: {e}")

if __name__ == "__main__":
    test_trulieve_fetch()