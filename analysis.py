import pandas as pd
import warnings
import re
import os
import datetime
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

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

def _standardize_types(df):
    """ Standardizes the 'Type' column to ensure consistent categories. Converts to lowercase and maps variations. """
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
    """ Main function to clean, analyze, and plot the scraped data. """
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
        # Close any open figures to conserve memory
        plt.close('all')
    print("\nAnalysis module executed.")
    # Return the cleaned dataframe
    return cleaned_df

def plot_brand_violin(data, category_name, save_dir):
    """
    Generates and saves a violin plot of Total Terps vs. Brand.
    (Implementation for Step 2)
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

def plot_top_50_heatmap(data, category_name, save_dir):
    """
    Generates and saves a heatmap of the top 50 terpiest products
    for a given category.
    (Implementation for Step 3)
    """
    print(f" > Plotting Top 50 Heatmap for {category_name}...")

    # --- 1. Define Terpenes and Category-Specific Filters ---

    # Define the subset of terpenes we want to plot in the heatmap
    # (Based on the paCannabisDataAnalysis_all.m script)
    TERPS_TO_PLOT = [
        'beta-Myrcene', 'Limonene', 'beta-Caryophyllene', 'Terpinolene',
        'Linalool', 'alpha-Pinene', 'beta-Pinene', 'Humulene',
        'alpha-Bisabolol', 'Ocimene'
    ]

    # Define category-specific filtering logic
    # (Based on the paCannabisDataAnalysis_all.m script)
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
    (Implementation for Step 4)
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
    # Replicates MATLAB's: ax = axes('Position', [0.01 0.17 0.3 1])
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
    # Replicates MATLAB's: ax(2) = axes('Position', [0.325 0.02 0.55 0.925])
    ax_text = fig.add_axes([0.42, 0.0, 0.58, 1.0])
    ax_text.axis('off') # Hide axes

    # --- C. Draw the Text Lists ---
    # Replicates the nested text loop from MATLAB

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
