# google_sheets_writer.py
# This file will contain all the logic for authenticating
# with the Google Sheets API and writing our data.

import gspread
import pandas as pd
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
import os.path
import datetime

# Define the scopes (what we want to access)
SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/drive.file']

def get_google_creds():
    """
    Handles the Google authentication flow using a service account.
    Returns authenticated credentials.
    """
    if not os.path.exists('credentials.json'):
        print("ERROR: credentials.json not found. Please follow the setup instructions in README.md.")
        return None

    try:
        creds = Credentials.from_service_account_file('credentials.json', scopes=SCOPES)
        return creds
    except Exception as e:
        print(f"Error loading credentials: {e}")
        return None

def get_or_create_worksheet(spreadsheet, sheet_name):
    """
    Gets a worksheet by name, creating it if it doesn't exist.
    """
    try:
        return spreadsheet.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        print(f"Creating new worksheet: '{sheet_name}'")
        return spreadsheet.add_worksheet(title=sheet_name, rows="100", cols="30") # Increased default cols

def write_to_google_sheet(dataframe, spreadsheet_id, latest_sheet_name, archive_sheet_name):
    """
    Authenticates with Google and writes a pandas DataFrame to two separate sheets
    in a Google Spreadsheet: one for the latest data (overwrite) and one for
    archived data (append).
    """
    creds = get_google_creds()
    if not creds:
        return

    try:
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(spreadsheet_id)

        # --- 1. Write to "Latest Data" Sheet (Overwrite) ---
        print(f"\n--- Writing to '{latest_sheet_name}' (Overwrite) ---")
        latest_sheet = get_or_create_worksheet(spreadsheet, latest_sheet_name)
        
        print("Clearing existing data...")
        latest_sheet.clear()

        # Ensure NaN is written as empty string
        dataframe.fillna('', inplace=True)

        print(f"Writing {len(dataframe)} rows...")
        set_with_dataframe(latest_sheet, dataframe, resize=True)
        print("Write to 'Latest Data' successful!")

        # --- 2. Write to "Archived Data" Sheet (Append) ---
        print(f"\n--- Writing to '{archive_sheet_name}' (Append) ---")
        archive_df = dataframe.copy()
        archive_df['ScrapeDate'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        archive_sheet = get_or_create_worksheet(spreadsheet, archive_sheet_name)

        # Get existing headers from the sheet
        try:
            existing_headers = archive_sheet.row_values(1)
        except gspread.exceptions.APIError:
             existing_headers = [] # Sheet is likely empty

        all_headers = archive_df.columns.tolist()

        # If the sheet is empty or headers are missing, write them
        if not existing_headers:
            print("Writing headers to new archive sheet...")
            archive_sheet.append_row(all_headers, value_input_option='USER_ENTERED')
            existing_headers = all_headers # Set headers for the next step

        # Reorder archive_df to match the sheet's header order
        archive_df = archive_df[existing_headers]
        
        print(f"Appending {len(archive_df)} new rows...")
        # Convert df to list of lists to append
        rows_to_append = archive_df.astype(str).values.tolist()
        archive_sheet.append_rows(rows_to_append, value_input_option='USER_ENTERED')
        print("Append to 'Archived Data' successful!")

    except gspread.exceptions.SpreadsheetNotFound:
        print(f"ERROR: Spreadsheet with ID '{spreadsheet_id}' not found.")
        print("Please make sure the ID is correct and the sheet has been shared with the service account email.")
    except Exception as e:
        print(f"An error occurred while writing to Google Sheets: {e}")
        print("Please check your Google Cloud credentials and API permissions.")
