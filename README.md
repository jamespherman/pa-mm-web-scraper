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

2.  **Run the Scraper:**
    ```bash
    python main.py
    ```

3.  **Run Unit Tests:**
    ```bash
    python -m unittest discover tests
    ```
