import json
import os
# We import the function from your scraper file
# We don't need to import DUTCHIE_STORES here, as the scraper file imports it for itself.
from scrapers2.dutchie_scraper import get_dutchie_data

def main():
    print("Starting Dutchie scraper...")
    
    # Call the function without arguments, as it's defined.
    # The get_dutchie_data function will use the DUTCHIE_STORES constant
    # defined within its own file ('scrapers/dutchie_scraper.py').
    all_products = get_dutchie_data()
    
    if all_products:
        # Renaming output to our new "raw" convention
        output_filename = 'dutchie_raw_products.json'
        print(f"\nScraping finished. Saving all {len(all_products)} raw product details to {output_filename}...")
        
        # Save the raw JSON data to a file
        with open(output_filename, 'w', encoding='utf-8') as f:
            json.dump(all_products, f, indent=2, ensure_ascii=False)
            
        print(f"Successfully saved data to {output_filename}.")
        print("Next step: Run 'python3 parse_dutchie.py' to process this file.")
    else:
        print("Scraping finished, but no data was returned.")

if __name__ == "__main__":
    main()
