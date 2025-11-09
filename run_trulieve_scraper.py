import json
import os
from scrapers.trulieve_scraper import get_trulieve_data

# ====================================================================
# TODO: YOU MUST EDIT THIS DICTIONARY
#
# Please find the store IDs from the Trulieve website network logs,
# just like you did for Cresco.
#
# Example: "Trulieve (Washington)": "1050"
# ====================================================================
TRULIEVE_STORES = {
    "Trulieve (Squirrel Hill)": "86",
    "Trulieve (North Shore)": "90"
}

# --- Main execution ---
if __name__ == "__main__":
    print("Starting Trulieve scraper...")
    
    # Filter out placeholder stores
    stores_to_scrape = {
        name: id for name, id in TRULIEVE_STORES.items() 
        if id != "REPLACE_ME"
    }

    if not stores_to_scrape:
        print("ERROR: Please edit 'run_trulieve_scraper.py' and add your TRULIEVE_STORES IDs.")
    else:
        all_products_df = get_trulieve_data(stores_to_scrape)

        if all_products_df is not None and not all_products_df.empty:
            print(f"\n--- Scraping Complete ---")
            print(f"Successfully fetched a total of {len(all_products_df)} products.")
            
            output_filename = "trulieve_all_products.json"
            
            all_products_df.to_json(
                output_filename,
                orient="records",
                indent=2
            )
            
            print(f"Successfully saved all products to {output_filename}")

        else:
            print("Scraping finished, but no data was returned.")
