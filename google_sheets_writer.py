# google_sheets_writer.py
# This file will contain all the logic for writing our data to Google Sheets.

import gspread
import pandas as pd
from gspread_dataframe import set_with_dataframe
import datetime

def get_or_create_worksheet(spreadsheet, sheet_name):
    """
    Gets a worksheet by name, creating it if it doesn't exist.
    """
    try:
        return spreadsheet.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        print(f"Creating new worksheet: '{sheet_name}'")
        return spreadsheet.add_worksheet(title=sheet_name, rows="100", cols="30")

def write_to_google_sheet(spreadsheet, dataframe):
    """
    Writes a pandas DataFrame to a single worksheet, overwriting any
    existing data.
    The 'spreadsheet' object is already authenticated and passed from main.py.
    """
    try:
        # --- 1. Define Sheet Name ---
        sheet_name = "Sheet1"

        # --- 2. Write to "Sheet1" (Overwrite) ---
        print(f"\n--- Writing to '{sheet_name}' (Overwrite) ---")
        
        # Get or create the worksheet
        data_sheet = get_or_create_worksheet(spreadsheet, sheet_name)
        
        print("Clearing existing data...")
        data_sheet.clear()

        # Ensure NaN is written as empty string
        dataframe.fillna('', inplace=True)

        print(f"Writing {len(dataframe)} rows...")
        set_with_dataframe(data_sheet, dataframe, resize=True)
        print(f"Write to '{sheet_name}' successful!")

    except Exception as e:
        print(f"An error occurred while writing to Google Sheets: {e}")
        print("Please check your permissions for the spreadsheet.")