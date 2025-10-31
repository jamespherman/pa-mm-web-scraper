# PA Dispensary Web Scraper

This project is a Python-based tool to scrape product data (cannabinoids, terpenes, pricing) from various Pennsylvania dispensary websites. The goal is to provide a consolidated database for medical patients to find medication that meets their specific needs, mitigating the need to manually check multiple websites.

## Architecture

The scraper is designed with a modular, platform-based architecture. Each scraper file in the `scrapers/` directory corresponds to a specific dispensary platform API. The `main.py` script orchestrates the process, running each scraper in sequence and combining the data into a single, clean dataset.

### Scraped Platforms and Dispensaries

The following platforms are currently being scraped:

*   **iHeartJane (`iheartjane_scraper.py`)**
    *   Maitri
    *   Rise

*   **Dutchie (`dutchie_scraper.py`)**
    *   Ethos
    *   Liberty
    *   Ascend

*   **Trulieve API (`trulieve_scraper.py`)**
    *   Trulieve

*   **Cresco/Sunnyside API (`cresco_scraper.py`)**
    *   Sunnyside

## Usage

1.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

2. **Set up Google Sheets Integration (One-Time Setup):**
    a. **Create a Google Cloud Project:** Go to the [Google Cloud Console](https://console.cloud.google.com/) and create a new project.
    b. **Enable APIs:** In your new project, enable the **Google Drive API** and the **Google Sheets API**.
    c. **Create Service Account Credentials:**
        - Go to "APIs & Services" > "Credentials".
        - Click "Create Credentials" and select "Service Account".
        - Give the service account a name (e.g., "dispensary-scraper").
        - Grant the service account the "Editor" role to allow it to edit files.
        - Click "Done".
    d. **Generate a JSON Key:**
        - Find the service account you just created in the Credentials list.
        - Click on it, go to the "Keys" tab, and click "Add Key" > "Create new key".
        - Select "JSON" as the key type and click "Create". A `credentials.json` file will be downloaded.
    e. **Place the Credentials File:** Move the downloaded JSON file into the root directory of this project and rename it to `credentials.json`.
    f. **Create a Google Sheet:**
        - Create a new Google Sheet in your Google Drive.
        - Click the "Share" button in the top right.
        - Find the `client_email` from your `credentials.json` file and share the sheet with that email address, giving it "Editor" permissions.
    g. **Update `main.py`:**
        - Open `main.py` and replace the placeholder `"YOUR_SPREADSHEET_ID_HERE"` with the actual ID from your Google Sheet's URL. The ID is the long string of characters in the middle of the URL (e.g., `.../d/THIS_IS_THE_ID/edit...`).


3.  **Run the Scraper:**
    ```bash
    python main.py
    ```
    The first time you run this, you will be prompted to authenticate with Google in your browser. This will create a `token.json` file that will be used for future runs.

4.  **View the Data:**
    The script will write data to two sheets in your Google Sheet:
    *   **Latest Data:** This sheet is completely overwritten with the newest data every time the script runs.
    *   **Archived Data:** This sheet appends the new data from each run, along with a "ScrapeDate" column, creating a historical record.

5.  **Run Unit Tests:**
    ```bash
    python -m unittest discover tests
    ```
