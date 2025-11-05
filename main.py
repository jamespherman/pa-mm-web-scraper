# main.py
# This is the main script to run everything.

import datetime
import gspread
import os.path
import pandas as pd
from scrapers.iheartjane_scraper import fetch_iheartjane_data
from scrapers.dutchie_scraper import fetch_dutchie_data
from scrapers.trulieve_scraper import fetch_trulieve_data
from scrapers.cresco_scraper import fetch_cresco_data
from google_sheets_writer import write_to_google_sheet # We'll use this later

# --- Define Scopes for Google API ---
SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/drive.file']

# --- Define Stores ---
# We can build out this list from your MATLAB file
IHEARTJANE_STORES = {
    "Maitri (PGH)": 2913,
    "Rise": 2266,
}
TRULIEVE_STORES = {
    "Trulieve (Squirrel Hill)": "86",
    "Trulieve (North Shore)": "90"
}
CRESCO_STORES = {
    "Sunnyside (Penn Ave)": "203",
    "Sunnyside (Lawrenceville)": "89"
}

def main():
    # --- Google Sheets Authentication and Setup ---
    print("Authenticating with Google Sheets...")
    # Get current date for filename
    today_str = datetime.date.today().strftime('%Y-%m-%d')
    spreadsheet_title = f"PA_Scraped_Data_{today_str}"

    try:
        # Use gspread's OAuth2 flow (Desktop App method)
        # This uses credentials.json and token.json
        gc = gspread.oauth(
            credentials_filename='credentials.json',
            authorized_user_filename='token.json',
            scopes=SCOPES
        )

        # Check if a spreadsheet with today's date already exists
        try:
            spreadsheet = gc.open(spreadsheet_title)
            print(f"Spreadsheet '{spreadsheet_title}' already exists. Exiting.")
            return  # Use return to stop the script immediately
        except gspread.exceptions.SpreadsheetNotFound:
            # If it doesn't exist, create it.
            # It will be in your "My Drive" by default.
            print(f"Creating new spreadsheet: '{spreadsheet_title}'")
            spreadsheet = gc.create(spreadsheet_title)
            print(f"Spreadsheet created in 'My Drive': {spreadsheet.url}")

    except Exception as e:
        print(f"An error occurred during Google Sheets setup: {e}")
        print("Please ensure your 'credentials.json' and 'token.json' are set up correctly.")
        return # Exit if authentication fails

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
    dutchie_df = fetch_dutchie_data()
    if not dutchie_df.empty:
        all_dataframes.append(dutchie_df)

    # --- 3. Run Trulieve Scraper ---
    print("\nStarting Trulieve Scraper...")
    trulieve_df = fetch_trulieve_data(TRULIEVE_STORES)
    if not trulieve_df.empty:
        all_dataframes.append(trulieve_df)

    # --- 4. Run Cresco Scraper ---
    print("\nStarting Cresco Scraper...")
    cresco_df = fetch_cresco_data(CRESCO_STORES)
    if not cresco_df.empty:
        all_dataframes.append(cresco_df)

    if not all_dataframes:
        print("\nNo data was scraped from any source. Exiting.")
        return

    # --- 3. Combine Data ---
    print("\nCombining all data...")
    combined_df = pd.concat(all_dataframes, ignore_index=True)
    
    # --- 4. Clean and Standardize Data (Future) ---
    # Let's create our desired column order
    final_columns = [
        'Name', 'Brand', 'Store', 'Price', 'Weight', 'Weight_Str', 'dpg',
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

    # --- 6. Write to Google Sheets ---
    print("\nWriting to Google Sheets...")
    # The new function takes the spreadsheet object and the dataframe
    write_to_google_sheet(spreadsheet, combined_df)
    
    print("\nScraping complete!")

# This standard Python snippet means "run the main() function
# when this script is executed directly"
if __name__ == "__main__":
    main()
