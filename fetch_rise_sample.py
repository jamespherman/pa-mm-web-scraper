import requests
import json

# 1. The Single Index Endpoint (matches your screenshot "query" line)
url = "https://search.iheartjane.com/1/indexes/menu-products-production/query"

# 2. The Params (Algolia Agent string from your URL)
params = {
    "x-algolia-agent": "Algolia for JavaScript (4.20.0); Browser; instantsearch.js (4.60.0); react (18.3.0-canary-178c267a4e-20241218); react-instantsearch (7.3.0); react-instantsearch-core (7.3.0); next.js (14.2.25); JS Helper (3.15.0)"
}

# 3. The Headers (Exact match from your trace)
# Headers exactly as you found them (Note: Origin and Referer are critical)
headers = {
            'Accept': '*/*',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Origin': 'https.risecannabis.com',
            'Referer': 'https.risecannabis.com/',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15',
            'x-algolia-api-key': '11f0fcaee5ae875f14a915b07cb6ef27',
            'x-algolia-application-id': 'VFM4X0N23A'
        }

# 4. The Payload (Simple JSON, not nested in "requests")
# Even though content-type is form-urlencoded, Algolia often accepts a raw JSON string body.
payload = json.dumps({
    "query": "",
    "facets": ["*"],
    "filters": "store_id : 2266"
})

print(f"Testing Rise Algolia (Single Query) for Store 2266...")
print(f"Target URL: {url}")

try:
    # We use 'data=payload' to send the raw string body, NOT 'json=payload'
    # This respects the 'application/x-www-form-urlencoded' header while sending JSON content.
    response = requests.post(url, headers=headers, params=params, data=payload, timeout=10)
    
    print(f"Status Code: {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        hits = data.get("hits", [])
        print(f"Success! Found {len(hits)} products.")
        
        if hits:
            first_hit = hits[0]
            print("\n--- First Product Sample ---")
            print(f"Name: {first_hit.get('name')}")
            print(f"Brand: {first_hit.get('brand')}")
            print(f"Price: {first_hit.get('price_each')}")
            
            # Verify Terpenes exist
            lab_results = first_hit.get('lab_results', [])
            if lab_results:
                print(f"Lab Results Found: {len(lab_results)} items")
                # Print first few chemical names to be sure
                print("Chemicals:", [x.get('compound_name') for x in lab_results[0].get('lab_results', [])[:3]])
            else:
                print("Warning: No 'lab_results' found in this hit.")
    else:
        print("Error: Request failed.")
        print(response.text[:500])

except Exception as e:
    print(f"Critical Error: {e}")
