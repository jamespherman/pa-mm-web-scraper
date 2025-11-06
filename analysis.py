# analysis.py
# This module is responsible for all post-scraping data processing.
# It takes the combined, raw DataFrame from main.py and performs:
# 1. Data Cleaning: Converting data types, standardizing values.
# 2. Brand Consolidation: Merging brand name variations.
# 3. Product Name Cleaning: Removing clutter from product names.
# 4. Plotting and Visualization: Generating various plots to analyze the data,
#    which are then saved to the `figures/` directory.

import pandas as pd
import warnings
import re
import os
import datetime
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# A predefined list of known terpene columns. This ensures consistency
# when processing and plotting terpene-related data.
TERPENE_COLUMNS = [
    'beta-Myrcene', 'Limonene', 'beta-Caryophyllene', 'Terpinolene',
    'Linalool', 'alpha-Pinene', 'beta-Pinene', 'Caryophyllene Oxide',
    'Guaiol', 'Humulene', 'alpha-Bisabolol', 'Camphene', 'Ocimene',
    'Total_Terps'
]

# A predefined list of key cannabinoid columns.
CANNABINOID_COLUMNS = ['THC', 'THCa', 'CBD']

def _convert_to_numeric(df):
    """
    Converts key columns to a numeric type for calculations and analysis.

    This function iterates through predefined lists of columns (terpenes,
    cannabinoids, weight, price) and forces them to a numeric type. Any
    value that cannot be converted (e.g., an empty string) becomes `NaN`.
    It then calculates the 'dollars per gram' (dpg) metric and fills `NaN`
    values in terpene columns with 0.

    Args:
        df (pd.DataFrame): The DataFrame to process.

    Returns:
        pd.DataFrame: The DataFrame with specified columns converted to numeric types.
    """
    print("Converting data types to numeric...")
    numeric_cols = TERPENE_COLUMNS + CANNABINOID_COLUMNS + ['Weight', 'Price']

    # Ensure 'dpg' (dollars per gram) column exists before calculations
    if 'dpg' not in df.columns:
        df['dpg'] = pd.Series(dtype='float64')

    for col in numeric_cols:
        if col in df.columns:
            # `pd.to_numeric` with `errors='coerce'` is a robust way to convert.
            # It handles various data types and turns failures into Not a Number (NaN).
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Calculate 'dpg' only after Price and Weight are numeric
    df['dpg'] = df['Price'] / df['Weight']

    # For aggregation and analysis, NaN in terpene columns is not useful.
    # We replace it with 0, assuming that a missing value means zero concentration.
    df[TERPENE_COLUMNS] = df[TERPENE_COLUMNS].fillna(0)
    return df

def _consolidate_brands(df):
    """
    Standardizes brand names by mapping variations to a single canonical name.

    Dispensary menus often list the same brand under different names (e.g.,
    "&Shine", "Rythm", "Good Green" are all part of GTI). This function
    uses a predefined dictionary to consolidate these variations, making
    brand-level analysis more accurate.

    Args:
        df (pd.DataFrame): The DataFrame with the 'Brand' column to clean.

    Returns:
        pd.DataFrame: The DataFrame with consolidated brand names.
    """
    print("Consolidating brand names...")
    # The `brand_map` dictionary defines the desired transformations.
    # Key: The name found in the raw data. Value: The standardized name.
    brand_map = {
        # Variations of GTI
        'Good Green': 'GTI', '&Shine': 'GTI', 'Rythm': 'GTI', 'Rhythm': 'GTI',
        # Variations of Jushi
        'The Bank': 'Jushi', 'The Lab': 'Jushi', 'Seche': 'Jushi', 'Lab': 'Jushi',
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
        'Grass Roots': 'Curaleaf', 'Blades': 'Curaleaf', 'Select': 'Curaleaf', 'Select Briq': 'Curaleaf',
        # Variations of Verano
        'Essence': 'Verano', 'Savvy': 'Verano', 'Muv': 'Verano',
        # Variations of Vytal
        'Vytal Options': 'Vytal', 'mood by Vytal': 'Vytal', # Consolidate to Vytal
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
        'SupplyTM': 'Supply', # New addition
    }
    # The `.replace()` method on a pandas Series is highly efficient for this kind of mapping.
    df['Brand'] = df['Brand'].replace(brand_map)
    return df

def _clean_item_names(df):
    """
    Cleans product names by removing extraneous information like weight, type, etc.

    This function uses a series of regular expressions to strip common, non-descriptive
    terms from the 'Name' column, creating a new 'Name_Clean' column. This is
    useful for identifying unique strains and comparing products across brands.

    Args:
        df (pd.DataFrame): The DataFrame with the 'Name' column to clean.

    Returns:
        pd.DataFrame: The DataFrame with a new 'Name_Clean' column.
    """
    print("Cleaning item names...")
    if 'Name' not in df.columns:
        return df

    # Create a copy to avoid SettingWithCopyWarning
    names = df['Name'].copy()

    # 1. Translate MATLAB 'removeList' to a single regex 'or' (|) pattern
    # A list of common strings to remove from product names.
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

    # 2. A dictionary of regex patterns to remove weights, percentages, etc.
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

    # 3. Add the cleaned names as a new column and trim whitespace.
    df['Name_Clean'] = names.str.strip()

    # If cleaning resulted in an empty string, revert to the original name.
    df['Name_Clean'] = df.apply(
        lambda row: row['Name'] if not row['Name_Clean'] else row['Name_Clean'],
        axis=1
    )
    return df

def _standardize_types(df):
    """
    Standardizes product 'Type' column for consistent categorization.

    Maps variations like 'vape' or 'vapes' to a standard 'vaporizers' category.
    This is crucial for grouping data correctly before plotting.

    Args:
        df (pd.DataFrame): The DataFrame with the 'Type' column.

    Returns:
        pd.DataFrame: The DataFrame with standardized 'Type' values.
    """
    print("Standardizing product types...")
    if 'Type' not in df.columns:
        print("Warning: 'Type' column not found. Skipping type standardization.")
        return df
    # Ensure 'Type' is string and lowercase
    df['Type'] = df['Type'].astype(str).str.lower()
    # Define mappings for variations
    type_map = {
        'vape': 'vaporizers',
        'vapes': 'vaporizers',
        'concentrate': 'concentrates'
        # 'flower' is already consistent
    }
    df['Type'] = df['Type'].replace(type_map)
    return df

def run_analysis(dataframe):
    """
    The main orchestration function for the analysis module.

    It executes the cleaning and plotting functions in the correct order.

    Args:
        dataframe (pd.DataFrame): The raw, combined DataFrame from the scrapers.

    Returns:
        pd.DataFrame: The fully cleaned and processed DataFrame.
    """
    print("\n--- Starting Data Analysis Module ---")
    # Suppress common warnings from pandas/seaborn for cleaner output
    warnings.filterwarnings('ignore', category=FutureWarning)
    warnings.filterwarnings('ignore', category=UserWarning)
    # --- Step 1: Data Cleaning ---
    # Create a new, cleaned dataframe to avoid modifying the original
    cleaned_df = dataframe.copy()
    # Convert types first (critical for all other operations)
    cleaned_df = _convert_to_numeric(cleaned_df)
    # Standardize product types (e.g., 'vape' -> 'vaporizers')
    cleaned_df = _standardize_types(cleaned_df)
    # Consolidate brand names
    cleaned_df = _consolidate_brands(cleaned_df)
    # Clean item names
    cleaned_df = _clean_item_names(cleaned_df)
    print(f"Data cleaning complete. New column 'Name_Clean' added.")
    # --- Step 2: Plotting Orchestration ---
    # Create the date-stamped save directory
    today_str = datetime.date.today().strftime('%Y-%m-%d')
    save_dir = os.path.join('figures', today_str)
    os.makedirs(save_dir, exist_ok=True)
    print(f"\nSaving all plots to: {save_dir}")
    # Define the product categories we want to generate plots for
    CATEGORIES_TO_PLOT = ['flower', 'concentrates', 'vaporizers']
    for category in CATEGORIES_TO_PLOT:
        print(f"\n--- Analyzing Category: {category.upper()} ---")
        # Filter the DataFrame for the specific category
        category_df = cleaned_df[cleaned_df['Type'] == category].copy()
        if category_df.empty:
            print(f"No data found for category '{category}'. Skipping plots.")
            continue
        # --- Call plotting functions ---
        # Plot 1: Brand vs. Total Terpenes Violin Plot
        plot_brand_violin(category_df, category, save_dir)
        # Plot 2: Top 50 Terpiest Products Heatmap
        plot_top_50_heatmap(category_df, category, save_dir)
        # Plot 3: Dominant Terpene Summary Figure
        plot_dominant_terp_summary(category_df, category, save_dir)

        # Plot 4: Value (DPG) vs. Terps Scatter Plot
        plot_value_scatterplot(category_df, category, save_dir)

        # Plot 5: Top 25 Value Panel Chart
        plot_value_panel_chart(category_df, category, save_dir)

        # Close any open figures to conserve memory
        plt.close('all')
    print("\nAnalysis module executed.")
    # Return the cleaned dataframe
    return cleaned_df

def plot_brand_violin(data, category_name, save_dir):
    """
    Generates and saves a violin plot of Total Terps vs. Brand.

    This plot helps visualize the distribution of terpene content for each brand.
    Brands with fewer than a minimum number of samples are excluded.

    Args:
        data (pd.DataFrame): The data for a specific product category.
        category_name (str): The name of the category (e.g., 'flower').
        save_dir (str): The directory to save the plot image.
    """
    print(f" > Plotting Brand Violin for {category_name}...")

    # Define the minimum number of products a brand must have to be included
    MIN_SAMPLES = 5

    # --- 1. Filter and Prepare Data ---

    # Calculate product counts for each brand
    brand_counts = data['Brand'].value_counts()

    # Get a list of brands that meet the minimum sample requirement
    brands_to_keep = brand_counts[brand_counts >= MIN_SAMPLES].index

    if len(brands_to_keep) < 2:
        print(f" SKIPPING: Not enough brands (min 2) with >{MIN_SAMPLES} samples for {category_name}.")
        return

    # Filter the main DataFrame to only include these brands
    df_filtered = data[data['Brand'].isin(brands_to_keep)].copy()

    # --- 2. Create Sorting Order ---

    # Calculate the median 'Total_Terps' for each brand and sort
    brand_order = df_filtered.groupby('Brand')['Total_Terps'].median().sort_values().index

    # --- 3. Create Brand Labels with Counts (e.g., "Brand (N=5)") ---

    # Get the counts for the *filtered* list of brands
    final_counts = df_filtered['Brand'].value_counts()

    # Create new labels
    new_labels = [f"{brand} (N={final_counts[brand]})" for brand in brand_order]

    # --- 4. Plotting ---

    # Set plot style
    sns.set_style("whitegrid")

    # Define figure size
    # Adjust height dynamically based on the number of brands
    plot_height = max(7, len(brand_order) * 0.5)
    plt.figure(figsize=(12, plot_height))

    # Create the violin plot
    ax = sns.violinplot(
        data=df_filtered,
        x='Total_Terps', # Use Total_Terps on x-axis for horizontal plot
        y='Brand', # Use Brand on y-axis
        order=brand_order, # Apply the sorted brand order
        palette='viridis',
        inner='box', # Show a boxplot inside the violins
        orient='h', # Specify horizontal orientation
	cut=0
    )

    # Update y-tick labels to include counts
    ax.set_yticklabels(new_labels)

    # --- 5. Style and Save ---

    # Set titles and labels
    plt.title(f'Total Terpenes by Brand for {category_name.title()}', fontsize=16)
    plt.xlabel('Total Terpenes (%)', fontsize=12)
    plt.ylabel('Brand', fontsize=12)
    plt.xticks(fontsize=10)
    plt.yticks(fontsize=10)

    # Ensure layout is tight
    plt.tight_layout()

    # Define the output filename
    filename = os.path.join(save_dir, f'brand_terp_violin_{category_name}.png')

    # Save the figure
    try:
        plt.savefig(filename, dpi=150)
        print(f" SUCCESS: Saved plot to {filename}")
    except Exception as e:
        print(f" ERROR: Failed to save plot to {filename}. Reason: {e}")

    # Close the plot to free memory
    plt.close()

def plot_value_scatterplot(data, category_name, save_dir):
    """
    Generates a scatter plot of Price per Gram (DPG) vs. Total Terpenes.

    This plot helps identify which brands offer the best "value" in terms
    of terpene content for the price.

    Args:
        data (pd.DataFrame): The data for a specific product category.
        category_name (str): The name of the category.
        save_dir (str): The directory to save the plot.
    """
    print(f" > Plotting Value Scatter Plot for {category_name}...")

    # --- 1. Filter Data for Plotting ---

    # We must have valid DPG and Total_Terps to plot
    df_filtered = data[
        (data['dpg'].notna()) & (data['dpg'] > 0) &
        (data['Total_Terps'].notna()) & (data['Total_Terps'] > 0)
    ].copy()

    # --- 2. Remove Extreme Outliers for Readability ---

    # Calculate the 95th percentile for DPG and Terps
    dpg_limit = df_filtered['dpg'].quantile(0.95)
    terp_limit = df_filtered['Total_Terps'].quantile(0.95)

    # Filter to keep only data within these "reasonable" limits
    df_plot = df_filtered[
        (df_filtered['dpg'] <= dpg_limit) &
        (df_filtered['Total_Terps'] <= terp_limit)
    ]

    if df_plot.empty:
        print(f" SKIPPING: No valid DPG vs. Terpene data found for {category_name}.")
        return

    # --- 3. Plotting ---

    sns.set_style("whitegrid")
    plt.figure(figsize=(15, 10))

    # Create the scatter plot
    ax = sns.scatterplot(
        data=df_plot,
        x='dpg',
        y='Total_Terps',
        hue='Brand', # Color-code by Brand
        size='Weight', # Vary point size by Weight
        sizes=(50, 250),
        alpha=0.7,
        palette='viridis'
    )

    # --- 4. Style and Save ---

    plt.title(f'Value Plot: Price per Gram vs. Total Terpenes for {category_name.title()}', fontsize=16)
    plt.xlabel('Price per Gram (DPG)', fontsize=12)
    plt.ylabel('Total Terpenes (%)', fontsize=12)

    # Move the legend outside the plot (it will be very busy)
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0.)

    # Add annotations for the "Value Quadrants"
    # Get plot limits
    xlim = ax.get_xlim()
    ylim = ax.get_ylim()

    # Calculate midpoints
    x_mid = (xlim[0] + xlim[1]) / 2
    y_mid = (ylim[0] + ylim[1]) / 2

    # Add quadrant labels
    ax.text(xlim[0] + (x_mid * 0.05), y_mid, 'Higher\nValue\n(High Terps,\nLow Price)',
            fontsize=12, color='gray', ha='left', va='center', alpha=0.5)
    ax.text(xlim[1] - (x_mid * 0.05), y_mid, 'Lower\nValue\n(Low Terps,\nHigh Price)',
            fontsize=12, color='gray', ha='right', va='center', alpha=0.5)

    # Ensure layout accounts for the external legend
    plt.tight_layout()

    # Define the output filename
    filename = os.path.join(save_dir, f'value_scatterplot_{category_name}.png')

    # Save the figure
    try:
        # Use bbox_inches='tight' to include the legend in the saved file
        plt.savefig(filename, dpi=150, bbox_inches='tight')
        print(f" SUCCESS: Saved plot to {filename}")
    except Exception as e:
        print(f" ERROR: Failed to save plot to {filename}. Reason: {e}")

    # Close the plot to free memory
    plt.close()

def plot_top_50_heatmap(data, category_name, save_dir):
    """
    Generates a heatmap of the top 50 products with the highest total terpenes.

    This provides a detailed look at the terpene profiles of the most potent
    products available in a given category.

    Args:
        data (pd.DataFrame): The data for a specific product category.
        category_name (str): The name of the category.
        save_dir (str): The directory to save the plot.
    """
    print(f" > Plotting Top 50 Heatmap for {category_name}...")

    # --- 1. Define Terpenes and Category-Specific Filters ---

    # Define the subset of terpenes we want to plot in the heatmap
    TERPS_TO_PLOT = [
        'beta-Myrcene', 'Limonene', 'beta-Caryophyllene', 'Terpinolene',
        'Linalool', 'alpha-Pinene', 'beta-Pinene', 'Humulene',
        'alpha-Bisabolol', 'Ocimene'
    ]

    # Category-specific filters to remove outliers (e.g., infused flower)
    filters = {
        'flower': (
            (data['Total_Terps'] > 2) &
            (data['Total_Terps'] < 6) &
            (data['THC'] < 40) &
            (~data['Subtype'].str.contains('Infused', case=False, na=False))
        ),
        'concentrates': (
            (data['Total_Terps'] > 5)
        ),
        'vaporizers': (
            (data['Total_Terps'] > 5)  # Use same logic as concentrates
        )
    }

    # --- 2. Filter Data ---

    if category_name not in filters:
        print(f" SKIPPING: No filter logic defined for category '{category_name}'.")
        return

    df_filtered = data[filters[category_name]].copy()

    if df_filtered.empty:
        print(f" SKIPPING: No products met the filter criteria for {category_name}.")
        return

    # --- 3. Find Top 50 Unique Products ---

    # First, get unique products by 'Name_Clean', keeping the one with the highest terps
    df_unique = df_filtered.sort_values('Total_Terps', ascending=False) \
        .drop_duplicates('Name_Clean')

    # Now, get the top 50 from that unique list
    top_50_df = df_unique.nlargest(50, 'Total_Terps').sort_values('Total_Terps', ascending=False)

    if top_50_df.empty:
        print(f" SKIPPING: No unique products available for heatmap in {category_name}.")
        return

    # --- 4. Prepare Data for Plotting ---

    # Create the Y-axis labels (e.g., "Strain | Brand | 3.5% terps | 22.1% THC")
    y_labels = [
        f"{row['Name_Clean']} | {row['Brand']} | "
        f"{row['Total_Terps']:.2f}% terps | {row['THC']:.1f}% THC"
        for index, row in top_50_df.iterrows()
    ]

    # Get just the terpene data for the heatmap
    heatmap_data = top_50_df[TERPS_TO_PLOT]

    # Sort the terpene columns (X-axis) by their mean value, descending
    # (This groups the most prominent terpenes together)
    terp_order = heatmap_data.mean().sort_values(ascending=False).index
    heatmap_data_sorted = heatmap_data[terp_order]

    # --- 5. Plotting ---

    sns.set_style("white")  # Use a white background for heatmaps

    # Make plot height dynamic based on number of products
    plot_height = max(10, len(top_50_df) * 0.3)
    plt.figure(figsize=(15, plot_height))

    # Create the heatmap
    ax = sns.heatmap(
        heatmap_data_sorted,
        yticklabels=y_labels,
        cmap='viridis',
        annot=True,  # Show the values
        fmt=".2f",  # Format values to 2 decimal places
        linewidths=.5,
        annot_kws={"size": 8}  # Smaller font for annotations
    )

    # --- 6. Style and Save ---

    plt.title(f'Top {len(top_50_df)} Terpiest {category_name.title()} Products', fontsize=16)
    plt.xlabel('Terpene', fontsize=12)
    plt.ylabel('Product | Brand | Profile', fontsize=12)
    plt.xticks(rotation=45, ha='right', fontsize=10)
    plt.yticks(fontsize=9)
    ax.xaxis.tick_top()  # Move X-axis labels to the top
    ax.xaxis.set_label_position('top')

    plt.tight_layout()

    # Define the output filename
    filename = os.path.join(save_dir, f'top_50_heatmap_{category_name}.png')

    # Save the figure
    try:
        plt.savefig(filename, dpi=150)
        print(f" SUCCESS: Saved plot to {filename}")
    except Exception as e:
        print(f" ERROR: Failed to save plot to {filename}. Reason: {e}")

    # Close the plot to free memory
    plt.close()

def plot_dominant_terp_summary(data, category_name, save_dir):
    """
    Generates and saves the dominant terpene pie chart and top 10 lists.

    This provides a high-level overview of the terpene landscape for a product category.

    Args:
        data (pd.DataFrame): The data for a specific product category.
        category_name (str): The name of the category.
        save_dir (str): The directory to save the plot.
    """
    print(f" > Plotting Dominant Terp Summary for {category_name}...")

    # --- 1. Define Terpenes and Apply Filters ---

    # Define the subset of terpenes we want to analyze
    TERPS_TO_PLOT = [
        'beta-Myrcene', 'Limonene', 'beta-Caryophyllene', 'Terpinolene',
        'Linalool', 'alpha-Pinene', 'beta-Pinene', 'Humulene',
        'alpha-Bisabolol', 'Ocimene'
    ]

    # Use the same category-specific filters as the heatmap
    filters = {
        'flower': (
            (data['Total_Terps'] > 2) &
            (data['Total_Terps'] < 6) &
            (data['THC'] < 40) &
            (~data['Subtype'].str.contains('Infused', case=False, na=False))
        ),
        'concentrates': (
            (data['Total_Terps'] > 5)
        ),
        'vaporizers': (
            (data['Total_Terps'] > 5)
        )
    }

    if category_name not in filters:
        print(f" SKIPPING: No filter logic defined for category '{category_name}'.")
        return

    df_filtered = data[filters[category_name]].copy()

    # Ensure we only have data for the terpenes we want to plot
    df_terpenes = df_filtered[TERPS_TO_PLOT].copy()

    if df_terpenes.empty:
        print(f" SKIPPING: No products met the filter criteria for {category_name}.")
        return

    # --- 2. Calculate Data for Pie Chart ---

    # Find the name of the dominant (highest value) terpene for each product
    dominant_terpenes = df_terpenes.idxmax(axis=1)

    # Count the occurrences of each dominant terpene
    pie_data = dominant_terpenes.value_counts()

    # Create a color map for consistent colors across plots
    terp_palette = sns.color_palette('viridis', n_colors=len(TERPS_TO_PLOT))
    color_map = {terp: color for terp, color in zip(TERPS_TO_PLOT, terp_palette)}

    # Get colors in the correct order for the pie chart
    pie_colors = [color_map.get(terp, '#B0B0B0') for terp in pie_data.index]

    # --- 3. Calculate "Top 10" Lists ---

    top_10_lists = {}
    for terp in TERPS_TO_PLOT:
        # Sort by the current terpene, descending
        df_sorted = df_filtered.sort_values(terp, ascending=False)

        # Get unique products by 'Name_Clean'
        df_unique = df_sorted.drop_duplicates('Name_Clean')

        # Get the top 10
        top_10 = df_unique.nlargest(10, terp)

        # Format the strings
        product_strings = []
        for _, row in top_10.iterrows():
            brand_short = row['Brand'][:10] # Abbreviate brand name
            name_short = row['Name_Clean'][:25] # Abbreviate strain name

            s = (f"{row[terp]:.2f}% | {brand_short} | "
                 f"{name_short} | {row['THC']:.1f}% THC")
            product_strings.append(s)

        top_10_lists[terp] = product_strings

    # --- 4. Plotting ---

    sns.set_style("white")
    fig = plt.figure(figsize=(20, 12))

    # --- A. Pie Chart Axes (Left Side) ---
    ax_pie = fig.add_axes([0.01, 0.1, 0.4, 0.8])

    # Create the pie chart
    patches, texts, autotexts = ax_pie.pie(
        pie_data,
        labels=pie_data.index,
        autopct='%1.1f%%',
        colors=pie_colors,
        startangle=90,
        pctdistance=0.85
    )

    # Style the text
    for text in texts:
        text.set_fontsize(10)
    for autotext in autotexts:
        autotext.set_fontsize(9)
        autotext.set_color('white')

    ax_pie.set_title(f'Dominant Terpene Profile for {category_name.title()}', fontsize=16, pad=20)

    # --- B. Text List Axes (Right Side) ---
    ax_text = fig.add_axes([0.42, 0.0, 0.58, 1.0])
    ax_text.axis('off') # Hide axes

    # --- C. Draw the Text Lists ---
    num_columns = 2 # We will lay out the 10 lists in 2 columns
    terps_per_column = len(TERPS_TO_PLOT) // num_columns

    x_start_positions = [0.0, 0.5] # X-position for Column 1, Column 2
    y_pos = 0.95 # Start at the top
    y_step_header = 0.04
    y_step_line = 0.02

    current_terp_index = 0

    for col in range(num_columns):
        x_pos = x_start_positions[col]
        y_pos = 0.95 # Reset Y for each new column

        for i in range(terps_per_column):
            terp_name = TERPS_TO_PLOT[current_terp_index]

            # Draw Header (e.g., "beta-Myrcene")
            ax_text.text(x_pos, y_pos, terp_name,
                         fontweight='bold', fontsize=10, color=color_map[terp_name])
            y_pos -= (y_step_line * 1.5) # Extra space after header

            # Draw Top 10 List
            for line in top_10_lists[terp_name]:
                ax_text.text(x_pos, y_pos, line, fontsize=8, family='monospace')
                y_pos -= y_step_line

            y_pos -= y_step_header # Extra space between lists
            current_terp_index += 1

    # --- 5. Save Figure ---

    # Define the output filename
    filename = os.path.join(save_dir, f'dominant_terp_summary_{category_name}.png')

    # Save the figure
    try:
        plt.savefig(filename, dpi=150, bbox_inches='tight')
        print(f" SUCCESS: Saved plot to {filename}")
    except Exception as e:
        print(f" ERROR: Failed to save plot to {filename}. Reason: {e}")

    # Close the plot to free memory
    plt.close()

def plot_value_panel_chart(data, category_name, save_dir):
    """
    Generates a 3-panel chart of the Top 25 "Best Value" products,
    showing Value Score, Price (DPG), and Total Terpenes.
    (Implementation for Step 6)
    """
    print(f" > Plotting Top 25 Value Panel Chart for {category_name}...")

    # --- 1. Define Filters and Calculate Value Score ---

    # We must have valid DPG and Total_Terps to calculate value
    df_filtered = data[
    (data['dpg'].notna()) & (data['dpg'] > 0) &
    (data['Total_Terps'].notna()) & (data['Total_Terps'] > 0)
    ].copy()

    # Calculate the "Value Score" (Terps per Dollar)
    # A higher score is better
    df_filtered['Value_Score'] = df_filtered['Total_Terps'] / df_filtered['dpg']

    # --- 2. Get Top 25 Products ---

    # Sort by the new Value_Score and get the top 25
    df_top25 = df_filtered.nlargest(25, 'Value_Score')

    if df_top25.empty:
        print(f" SKIPPING: No products with valid Value Score for {category_name}.")
        return

    # Sort by score ascending for plotting (so #1 is at the top)
    df_top25 = df_top25.sort_values('Value_Score', ascending=True)

    # --- 3. Create Y-Axis Labels ---

    # Create labels: "Brand | Name_Clean"
    y_labels = [
    f"{row['Brand']} | {row['Name_Clean']}"
    for _, row in df_top25.iterrows()
    ]

    # --- 4. Plotting ---

    sns.set_style("whitegrid")

    # Create a figure with 3 subplots that share a Y-axis
    fig, (ax1, ax2, ax3) = plt.subplots(
    1, 3,
    figsize=(20, 14),
    sharey=True # This links all three Y-axes
    )

    # Set a main title for the entire figure
    fig.suptitle(f'Top 25 "Best Value" Products: {category_name.title()}',
    fontsize=20, y=1.02)

    # --- Plot 1: Value Score (Terps per Dollar) ---
    bars1 = ax1.barh(y_labels, df_top25['Value_Score'],
    color=sns.color_palette('viridis', n_colors=len(y_labels)))
    ax1.set_xlabel('Value Score (Terps per $)', fontsize=12)
    ax1.tick_params(axis='x', labelsize=10)
    ax1.grid(axis='x', linestyle='--', alpha=0.7)
    # Add data labels
    ax1.bar_label(bars1, fmt='%.2f', padding=2, fontsize=8)

    # --- Plot 2: Price per Gram (DPG) ---
    bars2 = ax2.barh(y_labels, df_top25['dpg'],
    color=sns.color_palette('rocket', n_colors=len(y_labels)))
    ax2.set_xlabel('Price per Gram (DPG $)', fontsize=12)
    ax2.tick_params(axis='x', labelsize=10)
    ax2.grid(axis='x', linestyle='--', alpha=0.7)
    # Add data labels
    ax2.bar_label(bars2, fmt='$%.2f', padding=2, fontsize=8)

    # --- Plot 3: Total Terpenes ---
    bars3 = ax3.barh(y_labels, df_top25['Total_Terps'],
    color=sns.color_palette('mako', n_colors=len(y_labels)))
    ax3.set_xlabel('Total Terpenes (%)', fontsize=12)
    ax3.tick_params(axis='x', labelsize=10)
    ax3.grid(axis='x', linestyle='--', alpha=0.7)
    # Add data labels
    ax3.bar_label(bars3, fmt='%.2f%%', padding=2, fontsize=8)

    # --- 5. Style and Save ---

    # Style Y-axis ticks (only visible on ax1)
    ax1.tick_params(axis='y', labelsize=9)

    # Ensure layout is tight
    plt.tight_layout(rect=[0, 0.03, 1, 0.98]) # Adjust rect for suptitle

    # Define the output filename
    filename = os.path.join(save_dir, f'top_25_value_panel_{category_name}.png')

    # Save the figure
    try:
        plt.savefig(filename, dpi=150)
        print(f" SUCCESS: Saved plot to {filename}")
    except Exception as e:
        print(f" ERROR: Failed to save plot to {filename}. Reason: {e}")

    # Close the plot to free memory
    plt.close()
