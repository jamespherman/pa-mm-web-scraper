# utils/find_sweed_stores.py
#
# This is a one-time utility script to find all Zen Leaf (Sweed)
# store IDs located in Pennsylvania.

import requests
import json
import time

# --- Config ---
URL_STORE_INFO = "https://web-ui-production.sweedpos.com/_api/proxy/Store/GetStoreInfo"

# Headers adapted from your browser inspection
HEADERS = {
    "Accept": "*/*",
    "Content-Type": "application/json",
    "Origin": "https://zenleafdispensaries.com",
    "Referer": "https://zenleafdispensaries.com/",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15",
    "StoreId": "" # Will be set in the loop
}

# The master list of all store IDs you found in organizationFulfillmentTypeInfos
ALL_STORE_IDS = [
    57, 78, 88, 92, 106, 143, 145, 146, 147, 148, 149, 150, 151, 152, 153, 
    154, 155, 156, 157, 158, 159, 160, 179, 180, 181, 182, 183, 184, 233, 
    279, 280, 281, 282, 283, 284, 285, 286, 287, 288, 289, 290, 291, 292, 
    293, 294, 295, 296, 297, 298, 299, 300, 301, 302, 303, 304, 305, 306, 
    307, 308, 309, 310, 311, 312, 313, 314, 315, 316, 317, 318, 319, 320, 
    321, 322, 323, 324, 325, 326, 327, 328, 329, 330, 331, 332, 333, 334, 
    335, 336, 337, 338, 339, 340, 341, 342, 343, 344, 345, 346, 347, 348, 
    349, 350, 351, 352, 353, 354, 355, 357, 379, 380, 382, 383, 384, 385, 
    389, 535, 536, 537, 538, 539, 540, 541, 542, 543, 544, 545, 547, 548, 
    552, 553, 554, 555, 570, 576, 577, 578, 579
]

def main():
    print(f"Querying {len(ALL_STORE_IDS)} total store IDs to find PA locations...")
    
    pa_stores = {}
    
    for store_id in ALL_STORE_IDS:
        headers = HEADERS.copy()
        headers["StoreId"] = str(store_id)
        
        # The payload is empty, as seen in your log (Content-Length: 2)
        payload = {} 
        
        try:
            response = requests.post(URL_STORE_INFO, headers=headers, json=payload, timeout=5)
            
            if response.status_code != 200:
                print(f"  - ID {store_id}: Failed with status {response.status_code}")
                continue
                
            data = response.json()
            
            region = data.get('location', {}).get('region', {}).get('name')
            
            if region == "Pennsylvania":
                name = data.get('name', 'Unknown PA Store')
                city = data.get('location', {}).get('city', {}).get('name')
                
                # Format a user-friendly name
                # e.g., "Zen Leaf (Pittsburgh - McKnight)"
                friendly_name = f"Zen Leaf ({city} - {name.replace('Zen Leaf ', '')})"
                
                print(f"  +++ FOUND PA STORE: {friendly_name} (ID: {store_id})")
                pa_stores[friendly_name] = store_id
            
            # Politeness delay
            time.sleep(0.1)

        except requests.exceptions.RequestException as e:
            print(f"  - ID {store_id}: Error - {e}")
            continue

    print("\n--- SCRIPT COMPLETE ---")
    print("\nCopy the dictionary below into `scrapers/sweed_scraper.py`:")
    print("\nSWED_STORES_TO_SCRAPE = {")
    for name, store_id in pa_stores.items():
        print(f'    "{name}": {store_id},')
    print("}")

if __name__ == "__main__":
    main()