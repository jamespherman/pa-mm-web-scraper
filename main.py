# main.py
# This is the main script to run everything.

import datetime
import gspread
import pandas as pd
from scrapers.iheartjane_scraper import fetch_iheartjane_data
from scrapers.dutchie_scraper import fetch_dutchie_data
from scrapers.trulieve_scraper import fetch_trulieve_data
from scrapers.cresco_scraper import fetch_cresco_data
from google_sheets_writer import write_to_google_sheet # We'll use this later
from analysis import run_analysis

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
    combined_df = None
    # --- Google Sheets Authentication and Setup ---
    print("Authenticating with Google Sheets...")
    # Define the spreadsheet title dynamically based on today's date
    today_str = datetime.date.today().strftime('%Y-%m-%d')
    spreadsheet_title = f'PA_Scraped_Data_{today_str}'

    # Use gspread's OAuth2 flow
    gc = gspread.oauth(
        credentials_filename='credentials.json',
        authorized_user_filename='token.json',
        scopes=SCOPES
    )

    try:
        # Try to open the spreadsheet
        spreadsheet = gc.open(spreadsheet_title)
        print(f"Found existing sheet: '{spreadsheet_title}'. Loading data.")

        # Load data from the "Sheet1" worksheet (to match google_sheets_writer.py)
        worksheet = spreadsheet.worksheet("Sheet1")
        data = worksheet.get_all_records()
        combined_df = pd.DataFrame(data)
        print(f"Data loaded successfully from Google Sheet ({len(combined_df)} rows).")

    except gspread.exceptions.SpreadsheetNotFound:
        # This is now the "scrape" part of our "load-or-scrape" logic
        print(f"Spreadsheet '{spreadsheet_title}' not found. Starting scraper.")

        # Create the new spreadsheet
        spreadsheet = gc.create(spreadsheet_title)
        print(f"Sheet created: {spreadsheet.url}")

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
    except Exception as e:
        print(f"An error occurred during Google Sheets setup: {e}")
        print("Please ensure your 'credentials.json' and 'token.json' are set up correctly.")
        return # Exit if authentication fails
    
    return combined_df

# This standard Python snippet means "run the main() function
# when this script is executed directly"
if __name__ == "__main__":
    combined_df = main()

    # --- Analysis Handoff ---
    if combined_df is not None and not combined_df.empty:
        print("\n--- Handing off to Analysis Module ---")
        # Pass the loaded/scraped dataframe to the new module
        cleaned_df = run_analysis(combined_df)

        print("\n--- Data Analysis (Post-Module) ---")
        print("DataFrame shape:", cleaned_df.shape)
        print("\nColumns and data types:")
        print(cleaned_df.info())
    else:
        # This handles cases where main() returns None or an empty df
        print("\nNo data found or loaded. Skipping analysis.")

    print("\n--- Script Finished ---")
