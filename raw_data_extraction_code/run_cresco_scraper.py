import json
import os
from scrapers2.cresco_scraper import get_cresco_data

# Your store dictionary, as you provided
CRESCO_STORES = {
    "Sunnyside (Penn Ave)": "203",
    "Sunnyside (Lawrenceville)": "899"
}

# --- Main execution ---
if __name__ == "__main__":
    print("Starting Cresco (Sunnyside) scraper...")
    
    # The scraper function now returns a raw list of product data
    raw_products_list = get_cresco_data(CRESCO_STORES)

    if raw_products_list:
        print(f"\n--- Scraping Complete ---")
        print(f"Successfully fetched a total of {len(raw_products_list)} raw product entries.")
        
        # --- Save Raw JSON ---
        output_filename = "cresco_raw_products.json"
        
        with open(output_filename, 'w', encoding='utf-8') as f:
            json.dump(raw_products_list, f, indent=2, ensure_ascii=False)
        
        print(f"Successfully saved all raw product data to {output_filename}")
        print(f"Next step: Run 'python3 parse_cresco.py' to process this file.")

    else:
        print("Scraping finished, but no data was returned.")