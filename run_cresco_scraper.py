import json
import os
from scrapers.fresco_scraper import get_cresco_data

# Your store dictionary, as you provided
CRESCO_STORES = {
    "Sunnyside (Penn Ave)": "203",
    "Sunnyside (Lawrenceville)": "899"
}

# --- Main execution ---
if __name__ == "__main__":
    print("Starting Cresco (Sunnyside) scraper...")
    
    # --- CHANGE ---
    # Instead of just sending a list of IDs, we send the whole dictionary.
    # This lets the scraper use the real store names in its logs.
    all_products_df = get_cresco_data(CRESCO_STORES)

    if all_products_df is not None and not all_products_df.empty:
        print(f"\n--- Scraping Complete ---")
        print(f"Successfully fetched a total of {len(all_products_df)} products.")
        
        # --- Save to JSON ---
        # The scraper returns a DataFrame. Let's convert it to JSON
        # so you can inspect its structure, just like we did with Dutchie.
        output_filename = "cresco_all_products.json"
        
        # 'records' orientation is a clean list of {col: value} objects
        all_products_df.to_json(
            output_filename,
            orient="records",
            indent=2
        )
        
        print(f"Successfully saved all products to {output_filename}")

    else:
        print("Scraping finished, but no data was returned.")