# google_sheets_writer.py
# This file will contain all the logic for authenticating
# with the Google Sheets API and writing our data.

import gspread
import pandas as pd
from gspread_dataframe import set_with_dataframe
# This is for the Google auth. We'll need to install one more library:
# pip install google-auth-oauthlib
# We'll use this instead of the old oauth2client
from google.oauth2.service_account import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import os.path

# Define the scopes (what we want to access)
SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/drive.file']

# We will build out the functions here.

def write_to_google_sheet(dataframe, sheet_title):
    """
    Authenticates with Google and writes a pandas DataFrame to a
    Google Sheet. It will find the sheet by title or create a new one.
    """
    print(f"Attempting to write data to Google Sheet: {sheet_title}")
    
    # --- Authentication logic will go here ---
    # We will build a more robust auth flow that saves a 'token.json'
    # so you don't have to re-auth every single time.
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            print("Credentials expired, refreshing...")
            try:
                creds.refresh(Request())
            except Exception as e:
                print(f"Error refreshing token: {e}")
                print("Please re-authenticate.")
                creds = None # Force re-authentication
        
        if not creds:
            print("No valid credentials found, starting auth flow...")
            # Use 'credentials.json' (from Google Cloud) for the flow
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    try:
        # Authorize gspread
        client = gspread.authorize(creds)
        
        # --- Find or Create Sheet logic ---
        try:
            sheet = client.open(sheet_title).sheet1
            print(f"Found existing sheet: '{sheet_title}'")
        except gspread.exceptions.SpreadsheetNotFound:
            print(f"Creating new sheet: '{sheet_title}'")
            sheet = client.create(sheet_title).sheet1
            
        # --- Write data ---
        print("Clearing existing data...")
        sheet.clear()
        
        print(f"Writing {len(dataframe)} rows of data...")
        # Set the DataFrame to the sheet
        set_with_dataframe(sheet, dataframe, resize=True)
        
        print("Data write successful!")

    except Exception as e:
        print(f"An error occurred while writing to Google Sheets: {e}")
        print("Please check your Google Cloud credentials and API permissions.")
