# main.py
# This is the main script to run everything.

import pandas as pd
from scrapers.iheartjane_scraper import fetch_iheartjane_data
from scrapers.dutchie_scraper import get_dutchie_data # <-- ADD THIS
# from google_sheets_writer import write_to_google_sheet # We'll use this later

# --- Define Stores ---
# We can build out this list from your MATLAB file
IHEARTJANE_STORES = {
    "Maitri (PGH)": 2913,
    "Liberty (PGH)": 4909,
    # "Rise": 2266, # Let's add more after we know this works
    # "OR McKnight": 3906,
}

def main():
    print("Starting the PA Dispensary Scraper...")
    
    all_dataframes = [] # A list to hold all our DataFrames
    
    # --- 1. Run iHeartJane Scrapers ---
    for store_name, store_id in IHEARTJANE_STORES.items():
        # Pass both name and ID to the scraper
        df = fetch_iheartjane_data(store_id, store_name)
        if not df.empty:
            all_dataframes.append(df)
            
    # --- 2. Run Dutchie Scraper ---
    print("\nStarting Dutchie Scraper...")
    dutchie_df = get_dutchie_data()
    if not dutchie_df.empty:
        all_dataframes.append(dutchie_df)

    # --- 3. Run Other Scrapers (Future) ---
    # ... (we will add cresco, etc. here) ...

    if not all_dataframes:
        print("\nNo data was scraped from any source. Exiting.")
        return

    # --- 3. Combine Data ---
    print("\nCombining all data...")
    combined_df = pd.concat(all_dataframes, ignore_index=True)
    
    # --- 4. Clean and Standardize Data (Future) ---
    # Let's create our desired column order
    final_columns = [
        'Name', 'Brand', 'Store', 'Price', 'Weight', 'Weight_Str',
        'Type', 'Subtype', 'THC', 'THCa', 'CBD', 'Total_Terps',
        # Add all the terpenes
        'beta-Myrcene', 'Limonene', 'beta-Caryophyllene', 'Terpinolene',
        'Linalool', 'alpha-Pinene', 'beta-Pinene', 'Caryophyllene Oxide',
        'Guaiol', 'Humulene', 'alpha-Bisabolol', 'Camphene', 'Ocimene'
    ]
    
    # Reorder columns, filling in missing ones with 'None'
    # This ensures all DataFrames have the same columns
    combined_df = combined_df.reindex(columns=final_columns)
    
    # --- 5. Show Results ---
    print("\n--- Scraping Summary ---")
    print(f"Total products found: {len(combined_df)}")
    
    print("\nFirst 10 rows of data:")
    # Use to_string to show more columns
    print(combined_df.head(10).to_string())
    
    print("\nData columns and types:")
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

