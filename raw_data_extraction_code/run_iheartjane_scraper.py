import json
import os
from scrapers2.iheartjane_scraper import get_jane_data

# Store dictionary as you provided
IHEARTJANE_STORES = {
    "Maitri (PGH)": 2913,
    "Rise": 2266,
}

# --- Main execution ---
if __name__ == "__main__":
    print("Starting iHeartJane scraper...")
    
    # The scraper function will now return a raw list of product data
    raw_products_list = get_jane_data(IHEARTJANE_STORES)

    if raw_products_list:
        print(f"\n--- Scraping Complete ---")
        print(f"Successfully fetched a total of {len(raw_products_list)} raw product entries.")
        
        # --- Save Raw JSON ---
        output_filename = "iheartjane_raw_products.json"
        
        with open(output_filename, 'w', encoding='utf-8') as f:
            json.dump(raw_products_list, f, indent=2, ensure_ascii=False)
        
        print(f"Successfully saved all raw product data to {output_filename}")
        print(f"Next step: Run 'python3 parse_iheartjane.py' to process this file.")

    else:
        print("Scraping finished, but no data was returned.")