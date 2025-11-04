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
    Writes a pandas DataFrame to two separate sheets in a Google Spreadsheet:
    one for the latest data (overwrite) and one for archived data (append).
    The 'spreadsheet' object is already authenticated and passed from main.py.
    """
    try:
        # --- 1. Define Sheet Names ---
        latest_sheet_name = "Latest Data"
        archive_sheet_name = "Archived Data"

        # --- 2. Write to "Latest Data" Sheet (Overwrite) ---
        print(f"\n--- Writing to '{latest_sheet_name}' (Overwrite) ---")
        latest_sheet = get_or_create_worksheet(spreadsheet, latest_sheet_name)
        
        print("Clearing existing data...")
        latest_sheet.clear()

        # Ensure NaN is written as empty string
        dataframe.fillna('', inplace=True)

        print(f"Writing {len(dataframe)} rows...")
        set_with_dataframe(latest_sheet, dataframe, resize=True)
        print("Write to 'Latest Data' successful!")

        # --- 3. Write to "Archived Data" Sheet (Append) ---
        print(f"\n--- Writing to '{archive_sheet_name}' (Append) ---")
        archive_df = dataframe.copy()
        archive_df['ScrapeDate'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        archive_sheet = get_or_create_worksheet(spreadsheet, archive_sheet_name)

        # Get existing headers from the sheet
        try:
            existing_headers = archive_sheet.row_values(1)
        except gspread.exceptions.APIError:
            existing_headers = [] # Sheet is likely empty

        # If the sheet is empty, write headers first
        if not existing_headers:
            print("Writing headers to new archive sheet...")
            # Use the columns from the dataframe with ScrapeDate
            headers_to_write = archive_df.columns.tolist()
            archive_sheet.append_row(headers_to_write, value_input_option='USER_ENTERED')
            existing_headers = headers_to_write

        # Reorder archive_df to match the sheet's header order before appending
        # This handles cases where columns might have been manually reordered in the sheet
        final_archive_df = archive_df.reindex(columns=existing_headers).fillna('')

        print(f"Appending {len(final_archive_df)} new rows...")
        # Convert df to list of lists to append
        rows_to_append = final_archive_df.astype(str).values.tolist()
        archive_sheet.append_rows(rows_to_append, value_input_option='USER_ENTERED')
        print("Append to 'Archived Data' successful!")

    except Exception as e:
        print(f"An error occurred while writing to Google Sheets: {e}")
        print("Please check your permissions for the spreadsheet.")
