import gspread
import pandas as pd
from gspread_dataframe import set_with_dataframe
import datetime

# Define the scopes (what we want to access)
SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/drive.file']

try:
    # Authenticate using the service account
    client = gspread.oauth(
        credentials_filename='credentials.json',
        authorized_user_filename='token.json',
        scopes=SCOPES
    )

    # Get today's date as a string
    today_str = datetime.date.today().strftime('%Y-%m-%d')

    # Define the new sheet name
    sheet_name = f'PA_Scraped_Data_{today_str}'

    # Create the new spreadsheet
    spreadsheet = client.create(sheet_name)

    # Create a simple test pandas.DataFrame
    df = pd.DataFrame({
        'Status': ['Success'],
        'Timestamp': [datetime.datetime.now().isoformat()]
    })

    # Get the first worksheet
    worksheet = spreadsheet.sheet1

    # Write the DataFrame to the worksheet
    set_with_dataframe(worksheet, df)

    print(f'Successfully created sheet in your "My Drive": {spreadsheet.url}')

except Exception as e:
    print(f"An error occurred: {e}")
