import pandas as pd
import warnings
import re

# Define known terpene columns
TERPENE_COLUMNS = [
    'beta-Myrcene', 'Limonene', 'beta-Caryophyllene', 'Terpinolene',
    'Linalool', 'alpha-Pinene', 'beta-Pinene', 'Caryophyllene Oxide',
    'Guaiol', 'Humulene', 'alpha-Bisabolol', 'Camphene', 'Ocimene',
    'Total_Terps'
]

# Define other key numeric columns
CANNABINOID_COLUMNS = ['THC', 'THCa', 'CBD']

def _convert_to_numeric(df):
    """
    Converts all cannabinoid, terpene, and weight columns to numeric.
    Replaces empty strings and non-numeric values with NaN.
    """
    print("Converting data types to numeric...")
    numeric_cols = TERPENE_COLUMNS + CANNABINOID_COLUMNS + ['Weight', 'Price']

    # Ensure 'dpg' (dollars per gram) column exists before calculations
    if 'dpg' not in df.columns:
        df['dpg'] = pd.Series(dtype='float64')

    for col in numeric_cols:
        if col in df.columns:
            # errors='coerce' turns non-numeric strings (like '') into NaN
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Calculate 'dpg' only after Price and Weight are numeric
    df['dpg'] = df['Price'] / df['Weight']

    # Fill NaN in terpene columns with 0 for aggregation/analysis
    df[TERPENE_COLUMNS] = df[TERPENE_COLUMNS].fillna(0)
    return df

def _consolidate_brands(df):
    """
    Merges variations of brand names into a single, standard name.
    Based on logic from readAndClean.m.
    """
    print("Consolidating brand names...")
    # Define brand mappings
    brand_map = {
        # Variations of GTI
        'Good Green': 'GTI', '&Shine': 'GTI', 'Rythm': 'GTI', 'Rhythm': 'GTI',
        # Variations of Jushi
        'The Bank': 'Jushi', 'The Lab': 'Jushi', 'Seche': 'Jushi',
        # Variations of Trulieve
        'TruFlower': 'Trulieve', 'Cultivar Collection': 'Trulieve',
        'Modern Flower': 'Trulieve', 'Avenue': 'Trulieve', 'Muse': 'Trulieve',
        'Moxie': 'Trulieve', 'Franklin Labs': 'Trulieve',
        'Khalifa Kush': 'Trulieve', 'Roll One (Trulieve)': 'Trulieve',
        # Variations of Ayr
        'Lost In Translation': 'Ayr', 'Revel': 'Ayr', 'Origyn': 'Ayr',
        'Seven Hills': 'Ayr', 'Kynd': 'Ayr',
        # Variations of Cresco
        'Supply/Cresco': 'Cresco', 'FloraCal': 'Cresco',
        'Cresco Labs': 'Cresco', 'Sunnyside': 'Cresco', # Store name also used as brand
        # Variations of Curaleaf
        'Grass Roots': 'Curaleaf', 'Blades': 'Curaleaf',
        # Variations of Verano
        'Essence': 'Verano', 'Savvy': 'Verano', 'Muv': 'Verano',
        # Misc
        'The Woods': 'Terrapin',
        'Cookies': 'Kind Tree', 'Gage': 'Kind Tree',
        'Standard Farms': 'Standard Farms', 'Old Pal': 'Standard Farms', 'Highsman': 'Standard Farms',
        'Tyson 2.0': 'Columbia Care', 'Triple Seven': 'Columbia Care', 'Classix': 'Columbia Care',
        'FarmaceuticalRx': 'FRX',
        'Maitri Medicinals': 'Maitri',
        'Natural Selections': 'Natural Selections',
        'Organic Remedies': 'Organic Remedies',
        'Penn Health Group': 'PHG',
        'Prime Wellness': 'Prime',
    }
    # Use .replace() on the 'Brand' column to apply the mapping
    # This is much faster than looping
    df['Brand'] = df['Brand'].replace(brand_map)
    return df

def _clean_item_names(df):
    """
    Cleans item 'Name' column based on MATALB 'removeList' and 'PAT' logic.
    Removes weights, types, and other clutter.
    """
    print("Cleaning item names...")
    if 'Name' not in df.columns:
        return df

    # Create a copy to avoid SettingWithCopyWarning
    names = df['Name'].copy()

    # 1. Translate MATLAB 'removeList' to a single regex 'or' (|) pattern
    # This is a partial list for demonstration.
    remove_strings = [
        "QUARTER IS SMALL BUDS", "Delta 9", "1/8", "Flower", "Postgame",
        "indica", "Indica", "Sativa", "sativa", "Halftime", "Cartridge",
        "cartridge", "LAST CALL", "Cart", "cart", "LLR", "concentrate",
        "DISTILLATE", "LIVE", "BUDDER", "pen", "DART", "RESIN", "AIRO",
        "POD", "MUST", "BE", "USED", "WITH", "DEVICE", "Disposable",
        "Live", "Badder", "Rosin", "Resin", "Small Buds", "SMALL BUDS",
        "CO2", "Oil", "Syringe", "RSO", "Full Spectrum", "iKrusher", "Pod",
        "Distillate", "Co2", "Rhythm", "LR", "Dart", "Tere", "Pax", "PAX",
        "Vape", "Plus", "Liquid", "Cliq", "HTE", "Crumble", "Wax",
        "Ground", "Pre Ground", "PreGround", "Preground", "pre ground", "preground",
        "Smalls", "Diamonds", "Sauce", "Shatter", "Finished", "Concentrate",
        "Sugar", "LX", "Infinity", "Cold Pressed Hash", "Cold Pressed",
        "Truflower", "OR", "Minis", "Belushi's Farms", "Belushi Farm's",
        "Crystalline", "[3]", "THC/ml", "THC/1 ml", "ml", "- 3.5",
        "Pre-Pack", "Pre Pack", "Woods Reserve", "Dry", "Au", "Tyson",
        "Tru ", "Cultivar Collection", "Prime Wellness", "Prime", " l ",
        "R.O.", "SAT/ IND", "SATIVA", "Select Grind", "Fine Grind", "UHP",
        "Crystal", "THCa", "FARMACEUTICAX", "VYTAL OPTIONS", "VAULT",
        "The Bank", "Last Call", "Mystic Spirit", "[I]", "[S]", "THC:",
        "THC/", "Shake", "Supply", "Solventless", "BX", "Flavored",
        "[ - ]", "()", "Energize", "[H]", "(I)", "(S)", "(H)", "Hybrid",
        "(Bag)", "Rest", r"\*", r"\$", "!", r"\^" # Escaped special chars
    ]

    # Escape special regex characters in the list and join
    remove_list_regex = r'\b(' + '|'.join(re.escape(s) for s in remove_strings) + r')\b'
    names = names.str.replace(remove_list_regex, '', flags=re.IGNORECASE)

    # 2. Translate MATLAB 'PAT' (patterns)
    # This is a simplified subset of the 'PAT' logic
    pattern_map = {
        r'\b\d{1,2}\.\d{1,2}g\b': '',  # e.g., 3.5g, 0.5g
        r'\b\d{1,2}g\b': '',          # e.g., 1g, 7g
        r'\b\d{1,4}mg\b': '',         # e.g., 500mg, 100mg
        r'\b\d{1,3}\s*mg\b': '',      # e.g., 500 mg
        r'\b\d{1,2}\.\d{1,2}%\b': '', # e.g., 20.5%
        r'\b\d{1,2}%\b': '',          # e.g., 21%
        r'\b\d{1,2}\s*THC\b': '',     # e.g., 20 THC
        r'\s*-\s*$': '',             # Remove trailing ' -'
        r'^\s*-\s*': '',             # Remove leading ' -'
        r'\s\s+': ' '                # Collapse multiple whitespaces
    }

    for pattern, replacement in pattern_map.items():
        names = names.str.replace(pattern, replacement, flags=re.IGNORECASE)

    # 3. Final cleanup (like strtrim)
    df['Name_Clean'] = names.str.strip()

    # Replace empty strings with original name if cleaning blanked it
    df['Name_Clean'] = df.apply(
        lambda row: row['Name'] if not row['Name_Clean'] else row['Name_Clean'],
        axis=1
    )
    return df

def run_analysis(dataframe):
    """
    Main function to clean, analyze, and plot the scraped data.
    """
    print("\n--- Starting Data Analysis Module ---")

    # Suppress common warnings from pandas/seaborn for cleaner output
    warnings.filterwarnings('ignore', category=FutureWarning)
    warnings.filterwarnings('ignore', category=UserWarning)

    # --- Step 2: Data Cleaning ---
    # Create a new, cleaned dataframe to avoid modifying the original
    cleaned_df = dataframe.copy()

    # Convert types first (critical for all other operations)
    cleaned_df = _convert_to_numeric(cleaned_df)

    # Consolidate brand names
    cleaned_df = _consolidate_brands(cleaned_df)

    # Clean item names
    cleaned_df = _clean_item_names(cleaned_df)

    print(f"Data cleaning complete. New column 'Name_Clean' added.")

    # --- Placeholder for Plotting Functions ---
    print("Analysis module executed.")

    # Return the cleaned dataframe
    return cleaned_df
