# main.py
# This is the main script to run everything.

import pandas as pd
from scrapers.iheartjane_scraper import fetch_iheartjane_data
# from google_sheets_writer import write_to_google_sheet # We'll use this later

# --- Define Stores ---
# We can build out this list from your MATLAB file
IHEARTJANE_STORES = {
    "Maitri (PGH)": 2913,
    "Liberty (PGH)": 4909,
    # "Rise": 2266,
    # "OR McKnight": 3906,
}

def main():
    print("Starting the PA Dispensary Scraper...")
    
    all_dataframes = [] # A list to hold all our DataFrames
    
    # --- 1. Run iHeartJane Scrapers ---
    for store_name, store_id in IHEARTJANE_STORES.items():
        df = fetch_iheartjane_data(store_id, store_name)
        if not df.empty:
            all_dataframes.append(df)
            
    # --- 2. Run Other Scrapers (Future) ---
    # ... (we will add dutchie, cresco, etc. here) ...

    if not all_dataframes:
        print("No data was scraped from any source.")
        return

    # --- 3. Combine Data ---
    print("Combining all data...")
    combined_df = pd.concat(all_dataframes, ignore_index=True)
    
    # --- 4. Clean and Standardize Data (Future) ---
    # ... (we will add more cleaning steps here) ...
    
    # --- 5. Show Results ---
    print("\n--- Scraping Summary ---")
    print(f"Total products found: {len(combined_df)}")
    print("\nFirst 5 rows of data:")
    print(combined_df.head())
    
    print("\nData columns and types:")
    # This will print a summary of all columns and non-null values
    combined_df.info()

    # --- 6. Write to Google Sheets (Future) ---
    # print("\nWriting to Google Sheets...")
    # sheet_title = f"PA Product Data - {pd.Timestamp.now().strftime('%Y-%m-%d')}"
    # write_to_google_sheet(combined_df, sheet_title)
    
    print("\nScraping complete!")

# This standard Python snippet means "run the main() function
# when this script is executed directly"
if __name__ == "__main__":
    main()