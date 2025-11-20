# analysis.py
# -----------------------------------------------------------------------------
# This module is the "Data Science" brain of the project.
#
# After all the scrapers have run and collected thousands of raw products,
# this script takes that messy data and turns it into clean, useful insights.
#
# It performs two main jobs:
# 1. Data Cleaning:
#    - Fixing bad data (e.g., converting "3.5g" text to the number 3.5).
#    - Standardizing brand names (e.g., "GTI" -> "Rythm").
#    - removing "clutter" from product names.
#
# 2. Visualization:
#    - Creating beautiful charts and graphs to answer questions like:
#      "Which brand has the highest terpenes?"
#      "What is the best value flower?"
#    - Saving these charts as images in the `figures/` folder.
# -----------------------------------------------------------------------------

import pandas as pd # Data manipulation
import warnings # To silence annoying but harmless warnings
import re # Regex for text patterns
import os # File system operations
import datetime # Date handling
import matplotlib.pyplot as plt # Core plotting library
import seaborn as sns # Pretty plotting library built on top of matplotlib
import numpy as np # Math functions
import matplotlib.colors as mcolors # Color handling
from matplotlib.colors import LinearSegmentedColormap # Custom color maps

# --- Constants ---

# A list of ALL standardized terpenes we expect from the scrapers.
# We use this list to make sure every product has these columns,
# even if the value is 0.
TERPENE_COLUMNS = [
    'alpha-Terpinene', 'alpha-Bisabolol', 'beta-Caryophyllene', 'beta-Myrcene',
    'Camphene', 'Carene', 'Caryophyllene Oxide', 'Eucalyptol', 'Farnesene',
    'Geraniol', 'Guaiol', 'Humulene', 'Limonene', 'Linalool', 'Ocimene',
    'p-Cymene', 'Terpineol', 'Terpinolene', 'trans-Nerolidol', 'gamma-Terpinene',
    'alpha-Pinene', 'beta-Pinene' # Source columns for Pinene aggregation
]

# A predefined list of key cannabinoid columns.
CANNABINOID_COLUMNS = ['THC', 'THCa', 'CBD', 'CBDa', 'CBG', 'CBGa', 'CBN', 'THCv']

# --- Cleaning Functions ---

def _fix_weights_from_name(df):
    """
    Retroactively fixes weights by reading the product 'Name'.

    Sometimes the scraper misses the weight field, or the website has it wrong.
    But the product Name usually has it right (e.g., "Blue Dream [3.5g]").
    This function looks for that pattern and updates the 'Weight' column.
    """
    print("  - Retroactively fixing weights from product 'Name' field...")
    
    # Regex to find weights in brackets.
    # pattern matches: "[number] [unit]" like "[3.5g]" or "[500mg]"
    pattern = re.compile(r'\[([\d\.]+)\s*(mg|g)\]', re.IGNORECASE)

    def parse_weight_from_name(name):
        # Search the product name for the pattern
        match = pattern.search(str(name))
        if not match:
            return None  # No weight found

        try:
            value = float(match.group(1)) # The number part
            unit = match.group(2).lower() # The unit part (g or mg)
            
            if unit == 'mg':
                return value / 1000.0  # Convert mg to grams
            elif unit == 'g':
                return value
        except:
            return None # Failed to parse
        return None

    # Apply this logic to every single row in the dataframe
    corrected_weights = df['Name'].apply(parse_weight_from_name)

    # Only update rows where we actually found a new weight
    mask = corrected_weights.notna()
    df.loc[mask, 'Weight'] = corrected_weights[mask]
    
    count = mask.sum()
    if count > 0:
        print(f"    - Corrected {count} product weights based on 'Name'.")
        
    return df
    
def _clean_product_names(df):
    """
    Creates a 'Name_Clean' column by stripping junk from the Name.
    
    We want "Blue Dream [3.5g] - Indica" to just become "Blue Dream".
    This helps us group products together later.
    """
    print("Creating 'Name_Clean' column...")
    
    # Start with a copy of the 'Name' column
    df['Name_Clean'] = df['Name'].astype(str).copy()
    
    # --- 1. Define Junk Patterns ---
    
    # Pattern for weights (3.5g, 500mg, 1/8 oz)
    weight_pattern = re.compile(
        r'(\d+\.?\d*\s*(g|mg|ml))|'
        r'(\d+\s*(gram|milligram))|'
        r'(1/8\s*oz|1/4\s*oz|1/2\s*oz)',
        re.IGNORECASE
    )
    
    # List of marketing fluff words to remove
    general_clutter_words = [
        'live resin budder', 'live sauce', 'live resin badder', 'live budder',
        'live sauce', 'refresh', 'live badder', 'badder', 'crumble',
         'mixed buds', 'sugar leaf', 'pre-roll', 'pre ground', 'pre pack',
         'all in one', 'all-in-one', 'Rise LLR', 'Rest LLR', 'live resin', 'aio',
        'live', 'vape', 'pen', 'small', 'ground', 'cured', 'liquid',
        'indica', 'sativa', 'hybrid', 'thc', 'cbd', 'cbn', 'cbg',
        '10pk', 'pack', '10', 'x',
        'disposable', 'dispo',
        'Rise', 'Rest', 'LLR',
        'cart', 'cartridge',
        'co2','1oz', 'buds', 'bud', 
        'flower', 'littles', 'pre',
    ]
    
    # Combine all fluff words into one big regex pattern
    general_clutter_pattern = re.compile(r'\b(' + '|'.join(general_clutter_words) + r')\b', re.IGNORECASE)

    # Pattern for special characters (|, -, (, ), etc.)
    char_pattern = re.compile(r'[|/()\[\]{}:-]+', re.IGNORECASE)
    
    # Pattern to clean up extra spaces (e.g., "Blue   Dream" -> "Blue Dream")
    space_pattern = re.compile(r'\s{2,}')

    # --- 2. Apply Cleaning ---
    print("  - Removing general clutter (weights, types, etc.)...")
    df['Name_Clean'] = df['Name_Clean'].str.replace(weight_pattern, '', regex=True)
    df['Name_Clean'] = df['Name_Clean'].str.replace(general_clutter_pattern, '', regex=True)
    df['Name_Clean'] = df['Name_Clean'].str.replace(char_pattern, ' ', regex=True)

    # --- 3. Remove Brand Name from Product Name ---
    # If the brand is "Insa" and the product is "Insa Blue Dream", we want just "Blue Dream".
    print("  - Removing row-specific brand names...")
    
    def remove_brand_from_name(row):
        name = row['Name_Clean']
        brand = str(row['Brand'])
        
        if pd.isna(brand) or not brand:
            return name
        
        # Create a regex for this specific brand name
        brand_pattern = re.compile(r'\b(' + re.escape(brand) + r')\b', re.IGNORECASE)
        return brand_pattern.sub('', name)

    df['Name_Clean'] = df.apply(remove_brand_from_name, axis=1)
    
    # --- 4. Final Polish ---
    # Remove extra whitespace
    df['Name_Clean'] = df['Name_Clean'].str.replace(space_pattern, ' ', regex=True).str.strip()
    
    print("  - 'Name_Clean' column created successfully.")
    return df

def _reclean_brands(df):
    """
    Performs a final cleanup of the 'Brand' column.
    This catches any inconsistencies that might have slipped through
    the individual scrapers.
    """
    print("  - Performing final re-cleaning of 'Brand' column...")
    
    # Dictionary mapping "Dirty Name" -> "Clean Name"
    RECLEAN_BRAND_MAP = {
        'Cresco ': 'Cresco',
        'FarmaceuticalRx': 'FarmaceuticalRX',
        'FloraCal™': 'FloraCal Farms',
        'KYND': 'Kynd',
        'Seche': 'SeChe',
        'Supply™': 'Supply',
        'Remedi™': 'Remedi',
        'The John Daly Collection by PHG': 'John Daly',
        "Jim's Stash of Good Ugly Flower": "Belushi's Farm",
        'FRX': 'FarmaceuticalRX',
        'The Woods Reserve': 'Woods Reserve',
        'Botanist': 'The Botanist',
        "Moxie - PA": "Moxie",
        'RYTHM': 'Rythm',
        'Flower by Edie Parker': 'Edie Parker',
        'Black Buddha Cannabis': 'Black Buddha'
    }

    # Apply the map
    df['Brand'] = df['Brand'].replace(RECLEAN_BRAND_MAP)
    
    # Remove rows that have no brand (we can't analyze them properly)
    original_count = len(df)
    df = df.dropna(subset=['Brand'])
    removed_count = original_count - len(df)
    if removed_count > 0:
        print(f"    - Removed {removed_count} rows with None/NaN Brand.")
        
    return df

def _convert_to_numeric(df):
    """
    Converts text columns to numbers (floats) so we can do math on them.
    Also handles 'NaN' (Not a Number) values.
    """
    print("Converting data types to numeric...")
    numeric_cols = TERPENE_COLUMNS + CANNABINOID_COLUMNS + ['Weight', 'Price']

    # Ensure 'dpg' column exists
    if 'dpg' not in df.columns:
        df['dpg'] = pd.Series(dtype='float64')

    for col in numeric_cols:
        if col in df.columns:
            # Convert to number. If it fails, set it to NaN.
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Calculate Dollars Per Gram (DPG)
    df['dpg'] = df['Price'] / df['Weight']

    # Fill missing terpene values with 0 (assumption: missing means not present)
    df = df.reindex(columns=df.columns.union(TERPENE_COLUMNS), fill_value=0)
    df[TERPENE_COLUMNS] = df[TERPENE_COLUMNS].fillna(0)

    # --- Pinene Aggregation ---
    # Combine Alpha and Beta Pinene into one "Pinene" total
    print("Aggregating Pinene columns...")
    df['Pinene'] = df['alpha-Pinene'] + df['beta-Pinene']
    df = df.drop(columns=['alpha-Pinene', 'beta-Pinene'])

    # --- Total Terps Calculation ---
    # Sum up all the individual terpenes
    print("Calculating Total_Terps...")
    terps_to_sum = [col for col in TERPENE_COLUMNS if col not in ['alpha-Pinene', 'beta-Pinene']]
    terps_to_sum.append('Pinene')
    df['Total_Terps'] = df[terps_to_sum].sum(axis=1)

    # --- TAC Calculation ---
    # Sum up all the cannabinoids
    df[CANNABINOID_COLUMNS] = df[CANNABINOID_COLUMNS].fillna(0)
    print("Calculating TAC...")
    df['TAC'] = df[CANNABINOID_COLUMNS].sum(axis=1)

    return df

def run_analysis(dataframe):
    """
    The Master Function.

    This function controls the entire analysis process:
    1. Cleans the data.
    2. Filters out bad data.
    3. Loops through categories (Flower, Vape, Concentrate).
    4. Generates all the plots.

    Args:
        dataframe (pd.DataFrame): The raw data from the scrapers.

    Returns:
        pd.DataFrame: The final, clean data.
    """
    print("\n--- Starting Data Analysis Module ---")
    
    # Turn off pandas warnings (they are noisy)
    warnings.filterwarnings('ignore', category=FutureWarning)
    warnings.filterwarnings('ignore', category=UserWarning)
    
    # --- Step 1: Data Cleaning ---
    cleaned_df = dataframe.copy()
    
    cleaned_df = _fix_weights_from_name(cleaned_df)
    cleaned_df = _convert_to_numeric(cleaned_df)
    cleaned_df = _clean_product_names(cleaned_df)
    cleaned_df = _reclean_brands(cleaned_df)
    
    # Remove products with 0% TAC (bad data)
    initial_count = len(cleaned_df)
    cleaned_df = cleaned_df[cleaned_df['TAC'] > 0].copy()
    if len(cleaned_df) < initial_count:
        print(f"  - Dropped {initial_count - len(cleaned_df)} products with 0% TAC.")
        
    # Filter extreme terpene outliers (> 18% is likely an error)
    cleaned_df = cleaned_df[cleaned_df['Total_Terps'] <= 18].copy()
    
    print(f"Data cleaning complete.")

    # --- Step 2: Plotting ---
    
    # Create folder for today's plots
    today_str = datetime.date.today().strftime('%Y-%m-%d')
    save_dir = os.path.join('figures', today_str)
    os.makedirs(save_dir, exist_ok=True)
    print(f"\nSaving all plots to: {save_dir}")
    
    CATEGORIES_TO_PLOT = ['Flower', 'Concentrates', 'Vaporizers']
    
    for category in CATEGORIES_TO_PLOT:
        print(f"\n--- Analyzing Category: {category.upper()} ---")

        # Filter data for just this category
        category_df = cleaned_df[cleaned_df['Type'] == category].copy()
        
        # Exclude specific subtypes (like "Ground Flower" or "Infused")
        exclude_keywords = ['infused', 'ground', 'flower']
        pattern = '|'.join(exclude_keywords)
        category_df = category_df[
            ~category_df['Subtype'].str.contains(pattern, case=False, na=False)
        ]

        if category_df.empty:
            print(f"No data found for category '{category}'. Skipping.")
            continue
        
        # Generate the 5 Standard Plots
        plot_brand_violin(category_df, category, save_dir)
        plot_top_50_heatmap(category_df, category, save_dir)
        plot_dominant_terp_summary(category_df, category, save_dir)
        plot_value_scatterplot(category_df, category, save_dir)
        plot_value_panel_chart(category_df, category, save_dir)

        plt.close('all') # Cleanup memory

    print("\nAnalysis module executed.")
    return cleaned_df

# --- Plotting Functions ---

def plot_brand_violin(data, category_name, save_dir):
    """
    Creates a Violin Plot: Distribution of Terpenes by Brand.
    Shows which brands tend to have higher or lower terpenes.
    """
    print(f"  > Plotting Brand Violin for {category_name}...")

    # Only keep products with some terpenes
    data = data[data['Total_Terps'] > 0].copy()
    if data.empty: return

    # Only include brands with at least 10 products (for statistical relevance)
    MIN_SAMPLES = 10
    brand_counts = data['Brand'].value_counts()
    brands_to_keep = brand_counts[brand_counts >= MIN_SAMPLES].index
    if len(brands_to_keep) < 2: return

    df_filtered = data[data['Brand'].isin(brands_to_keep)].copy()

    # Sort brands by median terpene content
    brand_order = df_filtered.groupby('Brand')['Total_Terps'].median().sort_values().index

    # Create labels with counts: "Brand (N=15)"
    final_counts = df_filtered['Brand'].value_counts()
    new_labels = [f"{brand} (N={final_counts[brand]})" for brand in brand_order]

    # Draw Plot
    sns.set_style("whitegrid")
    plot_height = max(7, len(brand_order) * 0.5)
    plt.figure(figsize=(12, plot_height))

    ax = sns.violinplot(
        data=df_filtered,
        x='Total_Terps',
        y='Brand',
        order=brand_order,
        palette='hsv',
        inner='box',
        orient='h',
        cut=0
    )
    ax.set_yticklabels(new_labels)

    plt.title(f'Total Terpenes by Brand for {category_name.title()}', fontsize=16)
    plt.xlabel('Total Terpenes (%)', fontsize=18)
    plt.ylabel('Brand', fontsize=18)
    plt.tight_layout()

    filename = os.path.join(save_dir, f'brand_terp_violin_{category_name}.png')
    plt.savefig(filename, dpi=150)
    plt.close()

def plot_value_scatterplot(data, category_name, save_dir):
    """
    Creates a Scatter Plot: Price vs. Terpenes.
    Identifies "Good Value" products (High Terps, Low Price).
    """
    print(f"  > Plotting Value Scatter Plot for {category_name}...")

    df_plot = data.dropna(subset=['Brand']).copy()
    if df_plot.empty: return

    unique_brands = sorted(df_plot['Brand'].unique())

    # Setup colors
    n_hues = 15
    custom_colors = plt.cm.hsv(np.linspace(0, 1, n_hues + 1))[:-1]
    grays = [(0,0,0), (0.25,0.25,0.25), (0.75,0.75,0.75), (1,1,1)]

    brand_style_map = {}
    for i, brand in enumerate(unique_brands):
        brand_style_map[brand] = {
            'color': custom_colors[i % len(custom_colors)],
            'alt_color': grays[i % len(grays)]
        }

    # Draw Plot
    sns.set_style("whitegrid")
    fig, ax = plt.subplots(figsize=(15, 10))

    for brand in unique_brands:
        brand_df = df_plot[df_plot['Brand'] == brand]
        style = brand_style_map[brand]
        ax.plot(
            brand_df['dpg'],
            brand_df['Total_Terps'],
            marker='o', linestyle='None', markersize=9, alpha=0.7,
            label=brand, color=style['color'],
            fillstyle='left', markerfacecoloralt=style['alt_color'],
            markeredgewidth=0.5, markeredgecolor='black'
        )

    ax.set_title(f'Value Plot: Price per Gram vs. Total Terpenes for {category_name.title()}', fontsize=16)
    ax.set_xlabel('Price per Gram (DPG)', fontsize=12)
    ax.set_ylabel('Total Terpenes (%)', fontsize=12)

    # Dynamic Legend Columns
    ncol = 3 if len(unique_brands) > 60 else (2 if len(unique_brands) > 30 else 1)
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', ncol=ncol)
    plt.tight_layout()

    filename = os.path.join(save_dir, f'value_scatterplot_{category_name}.png')
    plt.savefig(filename, dpi=150, bbox_inches='tight')
    plt.close()

def plot_top_50_heatmap(data, category_name, save_dir):
    """
    Creates a Heatmap: Top 50 Terpiest Products.
    Shows the terpene profile breakdown for the best products.
    """
    print(f"  > Plotting Top 50 Heatmap for {category_name}...")

    # Filters to ensure we pick valid "Flower" or "Concentrates"
    filters = {
        'Flower': ((data['Total_Terps'] > 2) & (data['Total_Terps'] < 6)),
        'Concentrates': (data['Total_Terps'] > 5),
        'Vaporizers': (data['Total_Terps'] > 5)
    }

    if category_name not in filters: return
    df_filtered = data[filters[category_name]].copy()

    # Get Top 50 Unique Products
    top_50 = df_filtered.sort_values('Total_Terps', ascending=False).drop_duplicates('Name_Clean').head(50)
    if top_50.empty: return

    # Create Labels
    y_labels = [f"{row['Name_Clean']} | {row['Brand']} | {row['Total_Terps']:.2f}%" for _, row in top_50.iterrows()]

    # Select Terpenes to Display
    plot_terps = ['beta-Myrcene', 'Limonene', 'beta-Caryophyllene', 'Terpinolene', 'Linalool', 'Pinene', 'Humulene', 'alpha-Bisabolol', 'Ocimene']
    heatmap_data = top_50[plot_terps]
    
    # Draw Plot
    sns.set_style("white")
    fig = plt.figure(figsize=(18, max(10, len(top_50)*0.3)))
    sns.heatmap(heatmap_data, yticklabels=y_labels, cmap='Greys', annot=True, fmt=".2f", linewidths=.5)
    
    plt.title(f'Top {len(top_50)} Terpiest {category_name.title()} Products', fontsize=18)
    plt.tight_layout()

    filename = os.path.join(save_dir, f'top_50_heatmap_{category_name}.png')
    plt.savefig(filename, dpi=150)
    plt.close()

def plot_dominant_terp_summary(data, category_name, save_dir):
    """
    Creates a Dashboard: Dominant Terpene Pie Chart + Top 10 Lists.
    """
    print(f"  > Plotting Dominant Terp Summary for {category_name}...")
    
    terps_to_analyze = ['beta-Myrcene', 'Limonene', 'beta-Caryophyllene', 'Terpinolene', 'Linalool', 'Pinene', 'Humulene', 'alpha-Bisabolol', 'Ocimene']

    # Filter out low-terp products
    df_filtered = data[data['Total_Terps'] > 0.25].copy()
    if df_filtered.empty: return

    # --- Pie Chart Data ---
    dominant_terps = df_filtered[terps_to_analyze].idxmax(axis=1)
    pie_data = dominant_terps.value_counts()

    # --- Top 10 Lists Data ---
    top_10_lists = {}
    for terp in terps_to_analyze:
        top = df_filtered.sort_values(terp, ascending=False).drop_duplicates('Name_Clean').head(10)
        top_10_lists[terp] = [f"{row[terp]:.2f}% | {row['Brand'][:10]} | {row['Name_Clean'][:25]}" for _, row in top.iterrows()]

    # Draw Plot
    fig = plt.figure(figsize=(21, 16))
    
    # Pie Chart (Left)
    ax_pie = fig.add_axes([0.01, 0.5, 0.35, 0.35])
    ax_pie.pie(pie_data, labels=None, startangle=90, colors=plt.cm.tab10.colors)
    ax_pie.set_title(f'Dominant Terpene Distribution', fontsize=24)
    ax_pie.legend([f"{n} ({p:.1f}%)" for n, p in zip(pie_data.index, (pie_data/pie_data.sum()*100))], loc="lower center", bbox_to_anchor=(0.5, -0.3))

    # Lists (Right)
    ax_text = fig.add_axes([0.36, 0.0, 0.64, 1.0])
    ax_text.axis('off')
    
    y_pos = 0.95
    x_pos = 0.0
    for i, terp in enumerate(terps_to_analyze):
        if i == 5: # Start new column
            x_pos = 0.5
            y_pos = 0.95

        ax_text.text(x_pos, y_pos, terp, fontweight='bold', fontsize=20, color='blue')
        y_pos -= 0.03
        for line in top_10_lists[terp]:
            ax_text.text(x_pos, y_pos, line, fontsize=12, family='monospace')
            y_pos -= 0.02
        y_pos -= 0.04

    filename = os.path.join(save_dir, f'dominant_terp_summary_{category_name}.png')
    plt.savefig(filename, dpi=150, bbox_inches='tight')
    plt.close()

def plot_value_panel_chart(data, category_name, save_dir):
    """
    Creates a 3-Panel Chart: Value Score, Price, and Terpenes.
    Shows the Top 25 "Best Value" products.
    """
    print(f"  > Plotting Top 25 Value Panel Chart for {category_name}...")

    df = data[(data['dpg'] > 0) & (data['Total_Terps'] > 0)].copy()
    df['Value_Score'] = df['Total_Terps'] / df['dpg']
    
    top_25 = df.drop_duplicates('Name_Clean').nlargest(25, 'Value_Score').sort_values('Value_Score', ascending=True)
    if top_25.empty: return

    y_labels = [f"{row['Brand']} | {row['Name_Clean']}" for _, row in top_25.iterrows()]

    # Draw Plot
    fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(20, 14), sharey=True)
    fig.suptitle(f'Top 25 Best Value Products: {category_name.title()}', fontsize=26)

    # Panel 1: Value Score
    ax1.barh(y_labels, top_25['Value_Score'], color='green')
    ax1.set_xlabel('Value Score (Terps / Price)', fontsize=18)

    # Panel 2: Price
    ax2.barh(y_labels, top_25['dpg'], color='red')
    ax2.set_xlabel('Price per Gram ($)', fontsize=18)

    # Panel 3: Terpenes
    ax3.barh(y_labels, top_25['Total_Terps'], color='blue')
    ax3.set_xlabel('Total Terpenes (%)', fontsize=18)

    # Cleanup styling
    for ax in [ax1, ax2, ax3]:
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
    
    plt.tight_layout()
    filename = os.path.join(save_dir, f'top_25_value_panel_{category_name}.png')
    plt.savefig(filename, dpi=150)
    plt.close()
