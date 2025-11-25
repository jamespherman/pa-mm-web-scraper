# main.py
# -----------------------------------------------------------------------------
# This script serves as the main entry point for the PA Dispensary Scraper application.
#
# Think of this file as the "conductor" of an orchestra. It doesn't play the
# instruments itself (the individual scrapers do that), but it tells them when
# to start, collects their output, and makes sure everything works together.
#
# Its main responsibilities are:
# 1. Authentication: Logging in to Google services so we can save our data.
# 2. Orchestration: Deciding whether to load existing data or run new scrapers.
# 3. Aggregation: Collecting data from all different sources into one big list.
# 4. Saving: Writing the results to a Google Sheet.
# 5. Handoff: Passing the data to the analysis module for charts and graphs.
# -----------------------------------------------------------------------------

import datetime  # Used to get the current date (e.g., for file naming).
import gspread   # A library to interact with Google Sheets.
import pandas as pd # A powerful library for data manipulation (like Excel for Python).
import json      # Used for working with JSON data formats.

# Import the specific functions that run the scrapers for each dispensary.
# These functions are defined in other files in the `scrapers/` directory.
from scrapers.iheartjane_scraper import fetch_iheartjane_data
from scrapers.dutchie_scraper import fetch_dutchie_data
from scrapers.trulieve_scraper import fetch_trulieve_data
from scrapers.cresco_scraper import fetch_cresco_data
from scrapers.sweed_scraper import fetch_sweed_data

# Import the helper function to write our data to Google Sheets.
from google_sheets_writer import write_to_google_sheet

# Import the function that performs data analysis and creates charts.
from analysis import run_analysis
from infographic_generator import generate_pdf_report

# --- Define Scopes for Google API ---
# "Scopes" are like permissions. They tell Google exactly what this program
# is allowed to do with your account.
# - `spreadsheets`: Allows the program to read and write Google Sheets.
# - `drive.file`: Allows the program to create new files in your Google Drive.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/drive.file']

# --- Define Store Mappings ---
# These dictionaries map a human-readable store name (e.g., "Trulieve (Camp Hill)")
# to the specific ID number that the website's API uses to identify that store.
# This allows us to ask the API for data from specific locations.

TRULIEVE_STORES = {
    "Trulieve (Camp Hill)": "88",
    "Trulieve (Coatesville)": "92",
    "Trulieve (Cranberry Township)": "87",
    "Trulieve (Harrisburg)": "89",
    "Trulieve (Johnstown)": "74",
    "Trulieve (King of Prussia (Henderson))": "106",
    "Trulieve (Lancaster)": "76",
    "Trulieve (Limerick)": "109",
    "Trulieve (Philadelphia)": "107",
    "Trulieve (Philadelphia (Center City))": "104",
    "Trulieve (Philadelphia (Washington Square))": "85",
    "Trulieve (Pittsburgh (North Shore))": "90",
    "Trulieve (Pittsburgh (Squirrel Hill))": "86",
    "Trulieve (Reading (5th Street))": "84",
    "Trulieve (Reading (Lancaster Ave))": "80",
    "Trulieve (Scranton)": "108",
    "Trulieve (Washington)": "71",
    "Trulieve (Whitehall)": "79",
    "Trulieve (Wilkes-Barre)": "97",
    "Trulieve (York)": "81",
    "Trulieve (Zelienople)": "103",
}

CRESCO_STORES = {
    "Sunnyside (Butler)": "202",
    "Sunnyside (PGH - Penn Ave)": "203",
    "Sunnyside (New Kensington)": "229",
    "Sunnyside (Philly - Chestnut St)": "619",
    "Sunnyside (Wyomissing)": "624",
    "Sunnyside (Lancaster)": "633",
    "Sunnyside (Philly City Ave)": "634",
    "Sunnyside (Phoenixville)": "635",
    "Sunnyside (Montgomeryville)": "636",
    "Sunnyside (Ambler)": "650",
    "Sunnyside (Erie)": "785",
    "Sunnyside (Washington)": "813",
    "Sunnyside (Gettysburg)": "814",
    "Sunnyside (Somerset)": "815",
    "Sunnyside (Altoona)": "816",
    "Sunnyside (Greensburg)": "898",
    "Sunnyside (PGH - Lawrenceville)": "899",
    "Sunnyside (Beaver Falls)": "964"
}

def main():
    """
    The main function is the 'brain' of the operation.

    It performs the following steps:
    1. Authenticate with Google.
    2. Check if we have already scraped data for today.
       - If YES: Load that data instead of scraping again (saves time).
       - If NO: Run all the scrapers to get fresh data.
    3. Combine all the data into one big table.
    4. Write that table to a new Google Sheet.

    Returns:
        pd.DataFrame: A pandas DataFrame (a table of data) containing all the
                      product information. Returns None if something goes wrong.
    """
    combined_df = None # Initialize a variable to hold our final data

    # --- Google Sheets Authentication and Setup ---
    print("Authenticating with Google Sheets...")

    # Generate a title for our spreadsheet based on today's date.
    # Example: "PA_Scraped_Data_2023-10-27"
    # This allows us to keep a daily history of data.
    today_str = datetime.date.today().strftime('%Y-%m-%d')
    spreadsheet_title = f'PA_Scraped_Data_{today_str}'

    try:
        # `gspread.oauth()` handles the login process.
        # - `credentials_filename`: The 'key' we downloaded from Google Cloud.
        # - `authorized_user_filename`: A file that stores our login 'session' so
        #   we don't have to type our password every time.
        gc = gspread.oauth(
            credentials_filename='credentials.json',
            authorized_user_filename='token.json',
            scopes=SCOPES
        )

        # --- "LOAD" Logic ---
        # Try to open a spreadsheet with today's name.
        # If this line works, it means we already ran the scraper today!
        spreadsheet = gc.open(spreadsheet_title)
        print(f"Found existing sheet: '{spreadsheet_title}'. Loading data.")

        # Access the first tab (worksheet) of the spreadsheet.
        worksheet = spreadsheet.worksheet("Sheet1")

        # Download all the data from that sheet.
        data = worksheet.get_all_records()

        # Convert the data into a pandas DataFrame (our standard table format).
        combined_df = pd.DataFrame(data)
        print(f"Data loaded successfully from Google Sheet ({len(combined_df)} rows).")

    except gspread.exceptions.SpreadsheetNotFound:
        # --- "SCRAPE" Logic ---
        # If the spreadsheet was NOT found (SpreadsheetNotFound error),
        # it means we need to scrape fresh data.
        print(f"Spreadsheet '{spreadsheet_title}' not found. Starting scraper.")

        # Create a new, empty spreadsheet for today's data.
        spreadsheet = gc.create(spreadsheet_title)
        print(f"Sheet created: {spreadsheet.url}")

        print("Starting the PA Dispensary Scraper...")
        all_dataframes = []  # This list will collect the results from each scraper.

        # --- Run Individual Scrapers ---
        # We call each scraper function one by one.
        # Each function goes to a website, gets the data, and returns it as a DataFrame.
        
        print("\nStarting Dutchie Scraper...")
        dutchie_df = fetch_dutchie_data()
        if not dutchie_df.empty:
            all_dataframes.append(dutchie_df) # Add the result to our list
            
        print("\nStarting Sweed (Zen Leaf) Scraper...")
        sweed_df = fetch_sweed_data()
        if not sweed_df.empty:
            all_dataframes.append(sweed_df)
        
        print("\nStarting iHeartJane Scraper...")
        iheartjane_df = fetch_iheartjane_data()
        if not iheartjane_df.empty:
            all_dataframes.append(iheartjane_df)

        print("\nStarting Cresco Scraper...")
        cresco_df = fetch_cresco_data(CRESCO_STORES)
        if not cresco_df.empty:
            all_dataframes.append(cresco_df)

        print("\nStarting Trulieve Scraper...")
        trulieve_df = fetch_trulieve_data(TRULIEVE_STORES)
        if not trulieve_df.empty:
            all_dataframes.append(trulieve_df)
        
        # If we tried everything and got no data, stop here.
        if not all_dataframes:
            print("\nNo data was scraped from any source. Exiting.")
            return

        # --- Combine Data ---
        # Stack all the individual DataFrames on top of each other to make one big table.
        print("\nCombining all data...")
        combined_df = pd.concat(all_dataframes, ignore_index=True)

        # --- Define Final Column Structure ---
        # We want our final table to have a specific order of columns.
        # This makes the data easier to read and analyze.
        final_columns = [
            # Basic Product Info
            'Name', 'Brand', 'Store', 'Price', 'Weight', 'Weight_Str', 'dpg',
            'Type', 'Subtype',
            
            # Cannabinoids (Chemicals that get you high or give medical relief)
            'THC', 'THCa', 'CBD', 'CBDa', 'CBG', 'CBGa', 'CBN', 'THCv', 'Delta-8 THC', 'TAC',

            # Terpenes (Aromatic oils that affect the flavor and effect)
            'Total_Terps',
            'alpha-Terpinene',
            'alpha-Bisabolol',
            'beta-Caryophyllene',
            'beta-Myrcene',
            'Camphene',
            'Carene',
            'Caryophyllene Oxide',
            'Eucalyptol',
            'Farnesene',
            'Geraniol',
            'Guaiol',
            'Humulene',
            'Limonene',
            'Linalool',
            'Ocimene',
            'p-Cymene',
            'Terpineol',
            'Terpinolene',
            'trans-Nerolidol',
            'gamma-Terpinene',
            
            # Specific Pinene types (grouped together later in analysis)
            'alpha-Pinene',
            'beta-Pinene'
        ]
        
        # Reorganize the DataFrame to match our `final_columns` list.
        # If a column is missing (e.g., no products had 'Carene'), it will be created and filled with empty values.
        combined_df = combined_df.reindex(columns=final_columns)

        # --- Show Summary ---
        print("\n--- Scraping Summary ---")
        print(f"Total products found: {len(combined_df)}")

        # Print the first 10 rows so the user can verify it looks correct.
        print("\nFirst 10 rows of data:")
        print(combined_df.head(10).to_string())

        # Print technical info about the data types (e.g., numbers vs text).
        print("\nData columns and types:")
        combined_df.info()

        # --- Write to Google Sheets ---
        print("\nWriting to Google Sheets...")
        # Call our helper function to upload the data to the cloud.
        write_to_google_sheet(spreadsheet, combined_df)

    except FileNotFoundError:
        # This error happens if `credentials.json` is missing.
        print("\nERROR: 'credentials.json' not found.")
        print("Please follow the setup instructions in README.md to create this file.")
        return
    except Exception as e:
        # Catch-all for any other unexpected errors (like internet issues).
        print(f"\nAn unexpected error occurred: {e}")
        print("Please ensure your Google Cloud project is configured correctly and you have granted the necessary permissions.")
        return
    
    return combined_df

# --- Entry Point ---
# This block checks if the script is being run directly (not imported as a module).
# If it is run directly, it calls the `main()` function to start the program.
if __name__ == "__main__":
    combined_df = main()

    # --- Analysis Handoff ---
    # Once we have the data (either loaded or scraped), we pass it to the
    # analysis module to generate our plots and graphs.
    if combined_df is not None and not combined_df.empty:
        print("\n--- Handing off to Analysis Module ---")
        cleaned_df = run_analysis(combined_df)
        print("\n--- Analysis Complete ---")
        print("Cleaned DataFrame shape:", cleaned_df.shape)

        # --- PDF Generation Handoff ---
        if cleaned_df is not None and not cleaned_df.empty:
            generate_pdf_report(cleaned_df)
    else:
        print("\nNo data was loaded or scraped. Skipping analysis.")

    print("\n--- Script Finished ---")
