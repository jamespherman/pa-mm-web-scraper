# main.py
# This script serves as the main entry point for the PA Dispensary Scraper.
# It orchestrates the entire process, including:
# 1. Authenticating with the Google Sheets API.
# 2. Implementing a "load-or-scrape" logic:
#    - It first checks if a Google Sheet for the current date already exists.
#    - If yes, it loads the data from that sheet.
#    - If no, it runs the individual scrapers, combines the data, and writes
#      it to a newly created Google Sheet for the day.
# 3. Handoff to the analysis module (`analysis.py`) for data cleaning and visualization.

import datetime
import gspread
import pandas as pd
from scrapers.iheartjane_scraper import fetch_iheartjane_data
from scrapers.dutchie_scraper import fetch_dutchie_data
from scrapers.trulieve_scraper import fetch_trulieve_data
from scrapers.cresco_scraper import fetch_cresco_data
from google_sheets_writer import write_to_google_sheet
from analysis import run_analysis

# --- Define Scopes for Google API ---
# These scopes define the permissions the script will request from the user.
# - `spreadsheets`: Allows reading and writing spreadsheet data.
# - `drive.file`: Allows creating new spreadsheet files in the user's Drive.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/drive.file']

# --- Define Store Mappings ---
# These dictionaries map a user-friendly store name to the specific ID
# required by the respective scraper APIs.
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
    """
    The main function to orchestrate the scraping, data loading, and analysis process.
    """
    combined_df = None

    # --- Google Sheets Authentication and Setup ---
    print("Authenticating with Google Sheets...")
    # The spreadsheet title is generated dynamically to be unique for each day.
    # This forms the basis of our "load-or-scrape" logic.
    today_str = datetime.date.today().strftime('%Y-%m-%d')
    spreadsheet_title = f'PA_Scraped_Data_{today_str}'

    try:
        # `gspread.oauth()` handles the OAuth 2.0 "Desktop App" flow.
        # - `credentials_filename`: Points to the `credentials.json` file downloaded from Google Cloud.
        # - `authorized_user_filename`: Points to `token.json`, which stores the user's
        #   access and refresh tokens. This file is created automatically on the
        #   first successful authentication and reused on subsequent runs.
        gc = gspread.oauth(
            credentials_filename='credentials.json',
            authorized_user_filename='token.json',
            scopes=SCOPES
        )

        # --- "LOAD" Part of the "Load-or-Scrape" Logic ---
        # The script attempts to open a spreadsheet with today's date in the title.
        # If it succeeds, it means the data has already been scraped today.
        spreadsheet = gc.open(spreadsheet_title)
        print(f"Found existing sheet: '{spreadsheet_title}'. Loading data.")

        # Load data from the first worksheet.
        worksheet = spreadsheet.worksheet("Sheet1")
        data = worksheet.get_all_records()
        combined_df = pd.DataFrame(data)
        print(f"Data loaded successfully from Google Sheet ({len(combined_df)} rows).")

    except gspread.exceptions.SpreadsheetNotFound:
        # --- "SCRAPE" Part of the "Load-or-Scrape" Logic ---
        # If the spreadsheet is not found, it triggers the full scraping process.
        print(f"Spreadsheet '{spreadsheet_title}' not found. Starting scraper.")

        # A new spreadsheet is created with the dynamic title.
        # The `gspread` object (`gc`) is already authenticated from the `try` block.
        spreadsheet = gc.create(spreadsheet_title)
        print(f"Sheet created: {spreadsheet.url}")

        print("Starting the PA Dispensary Scraper...")
        all_dataframes = []  # A list to hold all our DataFrames

        # --- 1. Run Individual Scrapers ---
        # Each scraper function is called, and its resulting DataFrame is appended to the list.
        print("\nStarting iHeartJane Scraper...")
        for store_name, store_id in IHEARTJANE_STORES.items():
            df = fetch_iheartjane_data(store_id, store_name)
            if not df.empty:
                all_dataframes.append(df)

        print("\nStarting Cresco Scraper...")
        cresco_df = fetch_cresco_data(CRESCO_STORES)
        if not cresco_df.empty:
            all_dataframes.append(cresco_df)

        print("\nStarting Trulieve Scraper...")
        trulieve_df = fetch_trulieve_data(TRULIEVE_STORES)
        if not trulieve_df.empty:
            all_dataframes.append(trulieve_df)

        print("\nStarting Dutchie Scraper...")
        dutchie_df = fetch_dutchie_data()
        if not dutchie_df.empty:
            all_dataframes.append(dutchie_df)

        # If no scrapers return data, exit gracefully.
        if not all_dataframes:
            print("\nNo data was scraped from any source. Exiting.")
            return

        # --- 2. Combine Data ---
        print("\nCombining all data...")
        combined_df = pd.concat(all_dataframes, ignore_index=True)

        # --- 3. Define Final Column Structure ---
        # A predefined list of columns ensures a consistent structure for the final DataFrame.
        # This is important for both the Google Sheet and the analysis module.
        final_columns = [
            'Name', 'Brand', 'Store', 'Price', 'Weight', 'Weight_Str', 'dpg',
            'Type', 'Subtype', 'THC', 'THCa', 'CBD', 'Total_Terps',
            'beta-Myrcene', 'Limonene', 'beta-Caryophyllene', 'Terpinolene',
            'Linalool', 'alpha-Pinene', 'beta-Pinene', 'Caryophyllene Oxide',
            'Guaiol', 'Humulene', 'alpha-Bisabolol', 'Camphene', 'Ocimene'
        ]
        # `reindex` ensures all columns from `final_columns` are present, filling
        # any missing ones with NaN (which will become empty cells in the sheet).
        combined_df = combined_df.reindex(columns=final_columns)

        # --- 4. Show Summary ---
        print("\n--- Scraping Summary ---")
        print(f"Total products found: {len(combined_df)}")
        print("\nFirst 10 rows of data:")
        print(combined_df.head(10).to_string())
        print("\nData columns and types:")
        combined_df.info()

        # --- 5. Write to Google Sheets ---
        print("\nWriting to Google Sheets...")
        # The authenticated spreadsheet object and the combined data are passed
        # to the dedicated writer module.
        # This line can be commented out to run the scraper locally without cloud storage.
        write_to_google_sheet(spreadsheet, combined_df)

    except FileNotFoundError:
        print("\nERROR: 'credentials.json' not found.")
        print("Please follow the setup instructions in README.md to create this file.")
        return # Exit if credentials are not found
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")
        print("Please ensure your Google Cloud project is configured correctly and you have granted the necessary permissions.")
        return # Exit on other errors
    
    return combined_df

# This standard Python snippet ensures that the code inside this block only runs
# when the script is executed directly (e.g., `python main.py`).
if __name__ == "__main__":
    combined_df = main()

    # --- Analysis Handoff ---
    # After the main() function completes (either by loading or scraping),
    # the resulting DataFrame is passed to the analysis module.
    if combined_df is not None and not combined_df.empty:
        print("\n--- Handing off to Analysis Module ---")
        cleaned_df = run_analysis(combined_df)
        print("\n--- Analysis Complete ---")
        print("Cleaned DataFrame shape:", cleaned_df.shape)
    else:
        print("\nNo data was loaded or scraped. Skipping analysis.")

    print("\n--- Script Finished ---")
