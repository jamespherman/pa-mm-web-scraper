import json
import os
from scrapers2.trulieve_scraper import get_trulieve_data

TRULIEVE_STORES = {
    "Trulieve (Squirrel Hill)": "86",
    "Trulieve (North Shore)": "90"
}

# --- Main execution ---
if __name__ == "__main__":
    print("Starting Trulieve scraper...")
    
    stores_to_scrape = {
        name: id for name, id in TRULIEVE_STORES.items()
        if id != "REPLACE_ME"
    }

    if not stores_to_scrape:
        print("ERROR: Please edit 'run_trulieve_scraper.py' and add your TRULIEVE_STORES IDs.")
    else:
        # This function will now return a raw list of product dicts
        raw_products_list = get_trulieve_data(stores_to_scrape)

        if raw_products_list:
            print(f"\n--- Scraping Complete ---")
            print(f"Successfully fetched a total of {len(raw_products_list)} raw product entries.")
            
            output_filename = "trulieve_raw_products.json"
            
            with open(output_filename, 'w', encoding='utf-8') as f:
                json.dump(raw_products_list, f, indent=2, ensure_ascii=False)
            
            print(f"Successfully saved all raw product data to {output_filename}")
            print(f"Next step: Run 'python3 parse_trulieve.py' to process this file.")

        else:
            print("Scraping finished, but no data was returned.")
