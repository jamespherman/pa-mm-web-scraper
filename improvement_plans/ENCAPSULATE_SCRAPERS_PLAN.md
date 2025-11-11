# Refactor Plan: Encapsulate Scraper Store Lists

## 1. Goal

To refactor the "old" scrapers (`cresco`, `trulieve`, `iheartjane`) to match the cleaner, more modular design of the "new" scrapers (`dutchie`, `sweed`).

## 2. Rationale

This change will "encapsulate" all logic for a specific API (URLs, headers, and store lists) inside its own file. This simplifies `main.py` into a pure "orchestrator" and makes the entire project more consistent and easier to maintain.

## 3. Implementation Steps

This process will be repeated for `cresco_scraper.py`, `trulieve_scraper.py`, and `iheartjane_scraper.py`.

### Step 1: Modify the Scraper File (e.g., `scrapers/cresco_scraper.py`)

1.  **Move the Store Dictionary:** Copy the `CRESCO_STORES` dictionary from `main.py` and paste it into the top of `scrapers/cresco_scraper.py` (near the other constants).
2.  **Update the Function Definition:** Remove the `stores` argument from the main fetch function.
    * **Before:** `def fetch_cresco_data(stores):`
    * **After:** `def fetch_cresco_data():`
3.  **Update the Function Logic:** Change the function's main loop to iterate over its *own* internal `CRESCO_STORES` dictionary.
    * **Before:** `for store_name, store_id in stores.items():`
    * **After:** `for store_name, store_id in CRESCO_STORES.items():`

### Step 2: Update `main.py`

1.  **Remove the Store Dictionary:** Delete the `CRESCO_STORES`, `TRULIEVE_STORES`, and `IHEARTJANE_STORES` dictionaries from `main.py`.
2.  [cite_start]**Update the Function Calls:** Remove the arguments from the scraper function calls within the `main()` function [cite: 210-211].
    * **Before:**
        ```python
        print("\nStarting iHeartJane Scraper...")
        for store_name, store_id in IHEARTJANE_STORES.items():
            df = fetch_iheartjane_data(store_id, store_name)
            if not df.empty:
                all_dataframes.append(df)

        print("\nStarting Cresco Scraper...")
        cresco_df = fetch_cresco_data(CRESCO_STORES)
        if not cresco_df.empty:
            all_dataframes.append(cresco_df)

        print("\nStarting Trulieve Scraper...")
        trulieve_df = fetch_trulieve_data(TRULIEVE_STORES)
        if not trulieve_df.empty:
            all_dataframes.append(trulieve_df)
        ```
    * **After:**
        ```python
        print("\nStarting iHeartJane Scraper...")
        # (Note: iHeartJane's logic is slightly different, it will be refactored
        # to match the loop-inside-the-function pattern of the others)
        iheartjane_df = fetch_iheartjane_data() 
        if not iheartjane_df.empty:
            all_dataframes.append(iheartjane_df)

        print("\nStarting Cresco Scraper...")
        cresco_df = fetch_cresco_data()
        if not cresco_df.empty:
            all_dataframes.append(cresco_df)

        print("\nStarting Trulieve Scraper...")
        trulieve_df = fetch_trulieve_data()
        if not trulieve_df.empty:
            all_dataframes.append(trulieve_df)
        ```

*(Note: The `iheartjane_scraper.py` file will also need to be modified internally to loop over its new `IHEARTJANE_STORES` dictionary and return a single combined DataFrame, just as the other scrapers do.)*