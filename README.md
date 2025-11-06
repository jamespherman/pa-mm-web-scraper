# PA Dispensary Web Scraper

This project is a Python-based tool to scrape product data (cannabinoids, terpenes, pricing) from various Pennsylvania dispensary websites. The goal is to provide a consolidated database for medical patients to find medication that meets their specific needs, mitigating the need to manually check multiple websites.

## Architecture

The project is designed with a modular, multi-stage architecture:

1.  **Scraping (`scrapers/`)**: Each scraper file in the `scrapers/` directory corresponds to a specific dispensary platform API (e.g., iHeartJane, Dutchie). These modules are responsible for fetching raw product data.

2.  **Orchestration (`main.py`)**: The `main.py` script serves as the central controller. It performs the following key functions:
    *   **Google Authentication**: It handles the initial authentication with Google APIs.
    *   **Load or Scrape Logic**: It checks if a Google Sheet with today's date already exists. If it does, it loads the data directly from the sheet. If not, it runs the individual scrapers.
    *   **Data Aggregation**: It collects the data from all scrapers and combines them into a single pandas DataFrame.
    *   **Data Writing**: It passes the combined data to `google_sheets_writer.py` to be saved.
    *   **Analysis Handoff**: It passes the final DataFrame to the `analysis.py` module for cleaning and visualization.

3.  **Data Storage (`google_sheets_writer.py`)**: This module is responsible for all interactions with the Google Sheets API. It handles the creation of new spreadsheets and worksheets, and it writes the scraped data to the sheet.

4.  **Analysis & Visualization (`analysis.py`)**: This module takes the raw, combined data and performs several cleaning and standardization steps. It then generates and saves a series of plots and heatmaps to the `figures/` directory, providing insights into the scraped data.

### Scraped Platforms and Dispensaries

The following platforms are currently supported:

*   **iHeartJane (`iheartjane_scraper.py`)**: Maitri, Rise
*   **Dutchie (`dutchie_scraper.py`)**: Ethos, Liberty, Ascend
*   **Trulieve API (`trulieve_scraper.py`)**: Trulieve
*   **Cresco/Sunnyside API (`cresco_scraper.py`)**: Sunnyside

## Usage

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Set up Google Sheets Integration (Optional)

This project uses the Google Sheets API to store and retrieve scraped data. The script is configured to automatically create a new sheet for each day's scrape, titled `PA_Scraped_Data_YYYY-MM-DD`.

The authentication process uses an **OAuth 2.0 "Desktop App" flow**. The first time you run the script, it will open a browser window and ask you to log in to your Google account and grant permissions. This process will create a `token.json` file in the project directory, which will be used to automatically re-authenticate on subsequent runs.

To set this up, you need to provide your own Google Cloud API credentials:

1.  **Create a Google Cloud Project**: Go to the [Google Cloud Console](https://console.cloud.google.com/) and create a new project.
2.  **Enable APIs**: In your new project, go to the "APIs & Services" dashboard and enable the **Google Drive API** and the **Google Sheets API**.
3.  **Configure OAuth Consent Screen**:
    *   Go to "APIs & Services" > "OAuth consent screen".
    *   Choose **External** and click "Create".
    *   Fill in the required fields (App name, User support email, Developer contact).
    *   On the "Scopes" page, you can leave it blank.
    *   On the "Test users" page, add the email address of the Google account you intend to use for authentication.
4.  **Create OAuth 2.0 Credentials**:
    *   Go to "APIs & Services" > "Credentials".
    *   Click "+ CREATE CREDENTIALS" and select **OAuth client ID**.
    *   For the "Application type", select **Desktop app**.
    *   Give it a name (e.g., "Dispensary Scraper Client").
    *   After creation, a modal will appear. Click **DOWNLOAD JSON**.
5.  **Place the Credentials File**: Move the downloaded JSON file into the root directory of this project and rename it to `credentials.json`.

### 3. Run the Scraper

```bash
python main.py
```
*   **With Google Sheets**: If you completed the setup above, the script will authenticate and then either load today's data or run the scrapers and save the new data to a new Google Sheet.
*   **Without Google Sheets**: If you want to run the scraper and analyze the data locally without saving to the cloud, you can comment out the line `write_to_google_sheet(spreadsheet, combined_df)` in `main.py`. The script will still run the scrapers, perform the analysis, and save the plots to the `figures/` directory.

### 4. View the Data and Analysis

*   **Google Sheets**: A new spreadsheet titled `PA_Scraped_Data_YYYY-MM-DD` will be created in the Google Drive of the account you used to authenticate.
*   **Local Plots**: The analysis script will create a new date-stamped sub-directory inside the `figures/` folder (e.g., `figures/2023-10-27/`). This folder will contain all the generated plots, including:
    *   **Violin Plots**: Showing the distribution of total terpenes by brand for each product category.
    *   **Heatmaps**: Displaying the terpene profiles of the top 50 most potent products.
    *   **Scatter Plots**: Visualizing the "value" (price per gram vs. total terpenes) for different brands.
    *   **Summary Figures**: Including pie charts of dominant terpenes and top 10 product lists.

### 5. Run Unit Tests

To ensure all components are working as expected, you can run the test suite:
```bash
python -m unittest discover tests
```
