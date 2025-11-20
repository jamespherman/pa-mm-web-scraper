import requests
import json

url = "https://curaleaf.com/api-2/graphql"
headers = {
    'accept': '*/*',
    'content-type': 'application/json',
    'cookie': 'confirmed21OrOlder=1',  # <--- Vital for bypassing age gate
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'x-dutchie-session': 'eyJpZCI6IjE5NTA1MGVmLTQ2MzMtNGRhYS05YjA5LTc4MzQ1ZDU0MTlhMSIsImV4cGlyZXMiOjE3NjM0ODcxMDExMTd9'
}

# The Introspection Query (Standard)
query = """
query IntrospectionQuery {
  __schema {
    queryType {
      name
      fields {
        name
        args {
          name
        }
      }
    }
  }
}
"""

# Parameters must be URL-encoded for a GET request
params = {
    "operationName": "IntrospectionQuery",
    "variables": "{}",
    "query": query
}

print(f"Attempting Introspection (GET) on {url}...")

try:
    # CHANGED TO GET
    response = requests.get(url, headers=headers, params=params, timeout=10)
    
    print(f"Status Code: {response.status_code}")
    print(f"Raw Response Text (first 500 chars): {response.text[:500]}")

    response.raise_for_status()
    data = response.json()
    
    if 'errors' in data:
        print("\n❌ Introspection Failed (API returned errors):")
        print(json.dumps(data['errors'], indent=2))
    elif data.get('data', {}).get('__schema') is None:
        print("\n❌ Introspection Failed (Schema hidden or blocked).")
    else:
        print("\n✅ Introspection SUCCESS!")
        print("Scanning for 'filteredProducts'...")
        
        fields = data['data']['__schema']['queryType']['fields']
        for field in fields:
            if field['name'] == 'filteredProducts':
                print("\nFOUND 'filteredProducts'! Valid arguments:")
                for arg in field['args']:
                    print(f"  - {arg['name']}")
                break
            
except Exception as e:
    print(f"\n❌ Request Failed: {e}")
