# google_sheets_writer.py
# -----------------------------------------------------------------------------
# This module handles all interactions with Google Sheets.
#
# It's responsible for:
# 1. Finding the correct worksheet (tab) in the spreadsheet.
# 2. Creating a new worksheet if it doesn't exist.
# 3. Clearing old data to make room for the new scrape.
# 4. Writing the clean data to the sheet in a reliable way.
# -----------------------------------------------------------------------------

import gspread # The library that talks to Google's API.
import pandas as pd # Used to handle the data before writing.
from gspread_dataframe import set_with_dataframe # A helper to write pandas tables to sheets.

def get_or_create_worksheet(spreadsheet, sheet_name):
    """
    Finds a worksheet by name, or creates it if it's missing.

    Args:
        spreadsheet (gspread.Spreadsheet): The main spreadsheet object (the file).
        sheet_name (str): The name of the tab (e.g., "Sheet1").

    Returns:
        gspread.Worksheet: The worksheet object we can write to.
    """
    try:
        # Try to find the sheet
        return spreadsheet.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        # If it's not there, create it with a default size.
        print(f"Worksheet '{sheet_name}' not found. Creating a new one.")
        return spreadsheet.add_worksheet(title=sheet_name, rows="100", cols="30")

def write_to_google_sheet(spreadsheet, dataframe):
    """
    Writes our data table (DataFrame) to the Google Sheet.

    This effectively "Saves" our work to the cloud.

    Args:
        spreadsheet (gspread.Spreadsheet): The file to write to.
        dataframe (pd.DataFrame): The data to write.
    """
    try:
        # We always write to "Sheet1" for simplicity.
        sheet_name = "Sheet1"

        print(f"\n--- Preparing to write data to worksheet: '{sheet_name}' ---")
        
        # Get the sheet (or create it)
        worksheet = get_or_create_worksheet(spreadsheet, sheet_name)
        
        # Clear everything currently in the sheet.
        # This prevents old data from getting mixed up with new data.
        print("Clearing any existing data from the worksheet...")
        worksheet.clear()

        # Replace 'NaN' (Not a Number) values with empty strings ('').
        # Google Sheets doesn't like 'NaN' and makes the cell look ugly.
        # Empty strings just look like empty cells.
        dataframe_filled = dataframe.fillna('')

        # Write the data!
        # resize=True makes the sheet exactly the right size for our data.
        print(f"Writing {len(dataframe_filled)} rows and {len(dataframe_filled.columns)} columns...")
        set_with_dataframe(worksheet, dataframe_filled, resize=True)

        print(f"Successfully wrote data to '{sheet_name}'!")

    except Exception as e:
        # If something goes wrong (e.g., no internet, permission denied), tell the user.
        print(f"\nERROR: An error occurred while writing to Google Sheets: {e}")
        print("Please ensure you have 'Editor' permissions for the target spreadsheet.")
        # Print the URL so the user can check it manually.
        print(f"Spreadsheet URL: {spreadsheet.url}")
