import gspread
import pandas as pd
import gspread_dataframe
import datetime

# Define the scopes (must match main.py)
SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/drive.file']

# The name of the file that main.py just created
# (You might need to change 'Sheet1' to 'Latest Data' if you changed it back)
spreadsheet_title = "Sheet1" 

try:
    print("Authenticating with Google Sheets...")
    # Authenticate using the same (working) OAuth method
    client = gspread.oauth(
        credentials_filename='credentials.json',
        authorized_user_filename='token.json',
        scopes=SCOPES
    )

    print(f"Opening existing sheet: '{spreadsheet_title}'")
    spreadsheet = client.open(spreadsheet_title)

    print("Getting worksheet: 'Sheet1'")
    worksheet = spreadsheet.worksheet("Sheet1") # Or "Latest Data" if you changed it

    # --- THIS IS THE TEST ---
    # First, try the OLD (failing) method
    print("\nTesting OLD method (worksheet.get_all_records())...")
    old_data = worksheet.get_all_records()
    old_df = pd.DataFrame(old_data)
    print(f"DataFrame shape using OLD method: {old_df.shape}")

    # Second, try the NEW (hypothesized) method
    print("\nTesting NEW method (gspread_dataframe.get_as_dataframe())...")
    new_df = gspread_dataframe.get_as_dataframe(worksheet)
    print(f"DataFrame shape using NEW method: {new_df.shape}")
    # --- END OF TEST ---

    if new_df.shape[0] > 0:
        print("\n--- Hypothesis VERIFIED ---")
        print("gspread_dataframe.get_as_dataframe() successfully read the data.")
    else:
        print("\n--- Hypothesis FAILED ---")
        print("The new method also failed to read the data.")

except gspread.exceptions.WorksheetNotFound:
    print(f"Error: The worksheet 'Sheet1' was not found. This might be the bug.")
except Exception as e:
    print(f"An error occurred: {e}")