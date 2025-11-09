# google_sheets_writer.py
# This module provides a dedicated interface for interacting with Google Sheets.
# It encapsulates the logic for creating worksheets and writing pandas DataFrames
# to a specified Google Sheet, handling formatting and clearing of existing data.

import gspread
import pandas as pd
from gspread_dataframe import set_with_dataframe

def get_or_create_worksheet(spreadsheet, sheet_name):
    """
    Retrieves a worksheet by its name from a given spreadsheet.

    If the worksheet does not exist, it will be created with a default
    size of 100 rows and 30 columns. This prevents errors when trying to
    write to a non-existent sheet.

    Args:
        spreadsheet (gspread.Spreadsheet): The authenticated gspread Spreadsheet object.
        sheet_name (str): The name of the worksheet to find or create.

    Returns:
        gspread.Worksheet: The worksheet object.
    """
    try:
        # Attempt to retrieve the worksheet by its title.
        return spreadsheet.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        # If the worksheet doesn't exist, create it.
        print(f"Worksheet '{sheet_name}' not found. Creating a new one.")
        return spreadsheet.add_worksheet(title=sheet_name, rows="100", cols="30")

def write_to_google_sheet(spreadsheet, dataframe):
    """
    Writes a pandas DataFrame to a specified worksheet within a Google Sheet.

    This function will completely overwrite any existing data in the target worksheet.
    It uses the `gspread_dataframe` library for efficient writing. The function
    also handles the creation of the worksheet if it doesn't already exist.

    Args:
        spreadsheet (gspread.Spreadsheet): The authenticated gspread Spreadsheet
            object where the data will be written. This is passed from `main.py`.
        dataframe (pd.DataFrame): The pandas DataFrame containing the data to write.
    """
    try:
        # Define the target worksheet name.
        sheet_name = "Sheet1"

        print(f"\n--- Preparing to write data to worksheet: '{sheet_name}' ---")
        
        # Get the worksheet object, creating it if it doesn't exist.
        worksheet = get_or_create_worksheet(spreadsheet, sheet_name)
        
        # Clear all existing data from the worksheet to ensure a clean slate.
        print("Clearing any existing data from the worksheet...")
        worksheet.clear()

        # Replace any pandas `NaN` values with empty strings. This is because
        # `gspread` writes `NaN` as the string "#N/A" into the sheet, while
        # an empty string results in an empty cell, which is cleaner.
        dataframe_filled = dataframe.fillna('')

        # Use the `set_with_dataframe` function from the `gspread_dataframe`
        # library to write the entire DataFrame to the worksheet.
        # `resize=True` automatically adjusts the worksheet's dimensions to fit the DataFrame.
        print(f"Writing {len(dataframe_filled)} rows and {len(dataframe_filled.columns)} columns...")
        set_with_dataframe(worksheet, dataframe_filled, resize=True)

        print(f"Successfully wrote data to '{sheet_name}'!")

    except Exception as e:
        print(f"\nERROR: An error occurred while writing to Google Sheets: {e}")
        print("Please ensure you have 'Editor' permissions for the target spreadsheet.")
        print(f"Spreadsheet URL: {spreadsheet.url}")
