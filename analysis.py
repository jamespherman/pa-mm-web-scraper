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
import matplotlib.patches as mpatches
from matplotlib.colors import LinearSegmentedColormap
import matplotlib.colors as mcolors

# A list of ALL standardized terpenes we expect from the scrapers.
# This list is used to convert all columns to numeric and fill NaNs.
TERPENE_COLUMNS = [
    'alpha-Terpinene', 'alpha-Bisabolol', 'beta-Caryophyllene', 'beta-Myrcene',
    'Camphene', 'Carene', 'Caryophyllene Oxide', 'Eucalyptol', 'Farnesene',
    'Geraniol', 'Guaiol', 'Humulene', 'Limonene', 'Linalool', 'Ocimene',
    'p-Cymene', 'Terpineol', 'Terpinolene', 'trans-Nerolidol', 'gamma-Terpinene',
    'alpha-Pinene', 'beta-Pinene' # Source columns for Pinene aggregation
]

# A predefined list of key cannabinoid columns.
CANNABINOID_COLUMNS = ['THC', 'THCa', 'CBD']

def _clean_product_names(df):
    """
    Creates a 'Name_Clean' column by stripping clutter from the 'Name' column.
    
    This function removes:
    1. Weight patterns (e.g., "3.5g", "500mg")
    2. General clutter (e.g., "live", "vape", "indica")
    3. Special characters
    4. Row-specific brand names (e.g., removes "Insa" ONLY from Insa products)
    """
    print("Creating 'Name_Clean' column...")
    
    # Start with a copy of the 'Name' column
    df['Name_Clean'] = df['Name'].astype(str).copy()
    
    # --- 1. Define General Clutter Patterns ---
    
    # Pattern for weights (from our analysis)
    weight_pattern = re.compile(
        r'(\d+\.?\d*\s*(g|mg|ml))|'  # 3.5g, 500mg, 1ml
        r'(\d+\s*(gram|milligram))|' # 3 gram
        r'(1/8\s*oz|1/4\s*oz|1/2\s*oz)', # 1/8 oz
        re.IGNORECASE
    )
    
    # Pattern for general clutter (from our analysis)
    general_clutter_words = [
        'live', 'vape', 'pen', 'small', 'ground', 'cured', 'liquid',
        'indica', 'sativa', 'hybrid', 'thc', 'cbd', 'cbn', 'cbg',
        '10pk', 'pack', '10', 'x',
        'all-in-one', 'all in one', # For "All In One"
        'disposable', 'dispo',       # For "Disposable"
        
        # --- NEW FIX ---
        # Remove the specific phrases first
        'Rise LLR', 'Rest LLR',
        # Also remove the standalone words
        'Rise', 'Rest', 'LLR',
        # --- END FIX ---
        
        'cart', 'cartridge',        # For "Cart"
        'co2'                       # For "CO2"
    ]
    
    general_clutter_pattern = re.compile(r'\b(' + '|'.join(general_clutter_words) + r')\b', re.IGNORECASE)

    # Pattern for special characters and extra spaces
    char_pattern = re.compile(r'[|/()\[\]{}:-]+', re.IGNORECASE)
    space_pattern = re.compile(r'\s{2,}') # 2 or more spaces
    
    # --- 2. Apply General Cleaning Steps ---
    print("  - Removing general clutter (weights, types, etc.)...")
    df['Name_Clean'] = df['Name_Clean'].str.replace(weight_pattern, '', regex=True)
    df['Name_Clean'] = df['Name_Clean'].str.replace(general_clutter_pattern, '', regex=True)
    df['Name_Clean'] = df['Name_Clean'].str.replace(char_pattern, ' ', regex=True)

    # --- 3. Apply Row-Specific Brand Cleaning ---
    print("  - Removing row-specific brand names...")
    
    # This function is applied to each row.
    # It builds a specific regex for ONLY that row's brand.
    def remove_brand_from_name(row):
        name = row['Name_Clean']
        brand = str(row['Brand'])
        
        if pd.isna(brand) or not brand:
            return name
        
        # Create a regex pattern for just this row's brand
        # \b ensures we match "Insa" but not "Insane"
        brand_pattern = re.compile(r'\b(' + re.escape(brand) + r')\b', re.IGNORECASE)
        
        # Remove the brand from the name
        return brand_pattern.sub('', name)

    # Use .apply() to run this function on every row
    df['Name_Clean'] = df.apply(remove_brand_from_name, axis=1)
    
    # --- 4. Final Cleanup ---
    # Clean up extra spaces created by the removals
    df['Name_Clean'] = df['Name_Clean'].str.replace(space_pattern, ' ', regex=True).str.strip()
    
    print("  - 'Name_Clean' column created successfully.")
    return df
    
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
    # We use .reindex() to add any missing terpene columns as 0, preventing KeyErrors.
    df = df.reindex(columns=df.columns.union(TERPENE_COLUMNS), fill_value=0)
    df[TERPENE_COLUMNS] = df[TERPENE_COLUMNS].fillna(0)

    # --- Pinene Aggregation (as planned) ---
    print("Aggregating Pinene columns...")
    # Sum 'alpha-Pinene' and 'beta-Pinene' into a new 'Pinene' column
    df['Pinene'] = df['alpha-Pinene'] + df['beta-Pinene']
    
    # Drop the original source columns
    df = df.drop(columns=['alpha-Pinene', 'beta-Pinene'])
    # --- End Aggregation ---

    # --- Total Terps Calculation ---
    # Create the final list of columns to sum
    terps_to_sum = [col for col in TERPENE_COLUMNS if col not in ['alpha-Pinene', 'beta-Pinene']]
    terps_to_sum.append('Pinene') # Add our new aggregated column

    print("Calculating Total_Terps by summing all final terpene columns...")
    df['Total_Terps'] = df[terps_to_sum].sum(axis=1)
    # --- End Calculation ---

    return df

def run_analysis(dataframe):
    """
    The main orchestration function for the analysis module.

    It executes the cleaning and plotting functions in the correct order. This ensures that
    the data is properly prepared before visualization, and that all generated plots are
    saved to a date-stamped directory.

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
    
    # --- Product Name Cleaning ---
    # Create the 'Name_Clean' column for de-duplication
    cleaned_df = _clean_product_names(cleaned_df)
    
    print(f"Data cleaning complete.")
    
    # --- Step 2: Plotting Orchestration ---
    # Create the date-stamped save directory
    today_str = datetime.date.today().strftime('%Y-%m-%d')
    save_dir = os.path.join('figures', today_str)
    os.makedirs(save_dir, exist_ok=True)
    print(f"\nSaving all plots to: {save_dir}")
    
    # Define the product categories we want to generate plots for
    CATEGORIES_TO_PLOT = ['Flower', 'Concentrates', 'Vaporizers']
    
    # Loop over product categories to generate plots:
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
    Brands with fewer than a minimum number of samples are excluded to ensure
    statistical significance and readability of the plot.

    Args:
        data (pd.DataFrame): The data for a specific product category.
        category_name (str): The name of the category (e.g., 'flower').
        save_dir (str): The directory to save the plot image.
    """
    print(f"  > Plotting Brand Violin for {category_name}...")

    # Define the minimum number of products a brand must have to be included
    MIN_SAMPLES = 5

    # --- 1. Filter and Prepare Data ---

    # Calculate product counts for each brand
    brand_counts = data['Brand'].value_counts()

    # Get a list of brands that meet the minimum sample requirement
    brands_to_keep = brand_counts[brand_counts >= MIN_SAMPLES].index

    if len(brands_to_keep) < 2:
        print(f"    SKIPPING: Not enough brands (min 2) with >{MIN_SAMPLES} samples for {category_name}.")
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
        print(f"    SUCCESS: Saved plot to {filename}")
    except Exception as e:
        print(f"    ERROR: Failed to save plot to {filename}. Reason: {e}")

    # Close the plot to free memory
    plt.close()

def plot_value_scatterplot(data, category_name, save_dir):
    """
    Generates a scatter plot of Price per Gram (DPG) vs. Total Terpenes,
    using custom bi-colored markers for brand discrimination. This helps to
    visually identify which brands offer better value (higher terpenes for a
    lower price).

    Args:
        data (pd.DataFrame): The data for the specific product category.
        category_name (str): The name of the category (e.g., 'flower').
        save_dir (str): The directory where the plot image will be saved.
    """
    print(f"  > Plotting Value Scatter Plot for {category_name}...")

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
        print(f"    SKIPPING: No valid DPG vs. Terpene data found for {category_name}.")
        return

    # --- 3. Create Bi-Color & Fillstyle Map ---

    unique_brands = sorted(df_plot['Brand'].unique())
    n_brands = len(unique_brands)

    # Set legend columns dynamically based on number of brands
    if n_brands > 60:
        ncol = 3
    elif n_brands > 30:
        ncol = 2
    else:
        ncol = 1

    # Define the user-provided custom color lists
    # 11 Custom Colors (Primary)
    custom_colors = [
        (0.2941, 0, 0.5725),
        (0.3647, 0.2275, 0.6078),
        (0, 0.3529, 0.7098),
        (0.1020, 0.5216, 1.0000),
        (0.2510, 0.6902, 0.6510),
        (0.1020, 1.0000, 0.1020),
        (1.0000, 0.7608, 0.0392),
        (0.6000, 0.3098, 0),
        (0.8627, 0.1961, 0.1255),
        (0.9020, 0.3804, 0.3529),
        (0.8275, 0.3725, 0.7176)
    ]

    # 3 Grays (Secondary)
    grays = [
        (0.2, 0.2, 0.2), # User "light gray"
        (0.5, 0.5, 0.5), # User "mid gray"
        (0.8, 0.8, 0.8)  # User "dark gray"
    ]

    # We will use 'left' and 'right' fillstyles
    fillstyles = ['left', 'right']

    # Create the map for each brand
    brand_style_map = {}
    for i, brand in enumerate(unique_brands):
        brand_style_map[brand] = {
            'color': custom_colors[i % len(custom_colors)],
            'markerfacecoloralt': grays[i % len(grays)],
            'fillstyle': fillstyles[i % len(fillstyles)]
        }

    # --- 4. Plotting (Loop once per Brand, vectorized call) ---

    sns.set_style("whitegrid")
    fig, ax = plt.subplots(figsize=(15, 10))

    # Loop once per brand (fast) and plot all its data
    for brand in unique_brands:
        brand_df = df_plot[df_plot['Brand'] == brand]
        if brand_df.empty:
            continue

        style = brand_style_map[brand]

        ax.plot(
            brand_df['dpg'],
            brand_df['Total_Terps'],
            marker='o',
            linestyle='None', # This makes it a scatter plot
            markersize=9,
            alpha=0.7,
            label=brand,
            color=style['color'], # This is `markerfacecolor`
            fillstyle=style['fillstyle'],
            markerfacecoloralt=style['markerfacecoloralt'],
            markeredgewidth=0.5, # Add a thin edge
            markeredgecolor='black'
        )

    # --- 5. Style and Save ---

    ax.set_title(f'Value Plot: Price per Gram vs. Total Terpenes for {category_name.title()}', fontsize=16)
    ax.set_xlabel('Price per Gram (DPG)', fontsize=12)
    ax.set_ylabel('Total Terpenes (%)', fontsize=12)

    # Move the legend outside the plot
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0., ncol=ncol)

    # Add annotations for the "Value Quadrants"
    xlim = ax.get_xlim()
    ylim = ax.get_ylim()
    x_mid = (xlim[0] + xlim[1]) / 2
    y_mid = (ylim[0] + ylim[1]) / 2

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
        plt.savefig(filename, dpi=150, bbox_inches='tight')
        print(f"    SUCCESS: Saved plot to {filename}")
    except Exception as e:
        print(f"    ERROR: Failed to save plot to {filename}. Reason: {e}")

    # Close the plot to free memory
    plt.close()

def plot_top_50_heatmap(data, category_name, save_dir):
    """
    Generates a heatmap of the top 50 products with the highest total terpenes.

    This provides a detailed look at the terpene profiles of the most potent
    products available in a given category. Outliers (e.g., infused flower) are
    filtered out to ensure the comparison is meaningful.

    Args:
        data (pd.DataFrame): The data for a specific product category.
        category_name (str): The name of the category.
        save_dir (str): The directory to save the plot.
    """
    print(f"  > Plotting Top 50 Heatmap for {category_name}...")

    # --- 1. Define Terpenes and Category-Specific Filters ---

    # Define the subset of terpenes we want to plot in the heatmap
    TERPS_TO_PLOT = [
        'beta-Myrcene', 'Limonene', 'beta-Caryophyllene', 'Terpinolene',
        'Linalool', 'Pinene', 'Humulene', 'alpha-Bisabolol', 'Ocimene'
    ]

    # Category-specific filters to remove outliers (e.g., infused flower)
    filters = {
        'Flower': (
            (data['Total_Terps'] > 2) &
            (data['Total_Terps'] < 6) &
            (data['THC'] < 40) &
            (~data['Subtype'].str.contains('Infused', case=False, na=False))
        ),
        'Concentrates': (
            (data['Total_Terps'] > 5)
        ),
        'Vaporizers': (
            (data['Total_Terps'] > 5)  # Use same logic as concentrates
        )
    }

    # --- 2. Filter Data ---

    if category_name not in filters:
        print(f"    SKIPPING: No filter logic defined for category '{category_name}'.")
        return

    df_filtered = data[filters[category_name]].copy()

    if df_filtered.empty:
        print(f"    SKIPPING: No products met the filter criteria for {category_name}.")
        return

    # --- 3. Find Top 50 Unique Products ---

    # First, get unique products by 'Name_Clean', keeping the one with the highest terps
    df_unique = df_filtered.sort_values('Total_Terps', ascending=False) \
        .drop_duplicates('Name_Clean')

    # Now, get the top 50 from that unique list
    top_50_df = df_unique.nlargest(50, 'Total_Terps').sort_values('Total_Terps', ascending=False)

    if top_50_df.empty:
        print(f"    SKIPPING: No unique products available for heatmap in {category_name}.")
        return

    # --- 4. Prepare Data for Plotting ---

    # Create the Y-axis labels (e.g., "Strain | Brand | 3.5% terps | 22.1% THC")
    y_labels = []
    for index, row in top_50_df.iterrows():
        # Create the raw label string
        label_str = (f"{row['Name_Clean']} | {row['Brand']} | "
                     f"{row['Total_Terps']:.2f}% terps | {row['THC']:.1f}% THC")
        
        # Clean up any repeating "|" characters
        clean_label = re.sub(r'\|+', '|', label_str)
        y_labels.append(clean_label)

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
    fig = plt.figure(figsize=(18, plot_height))

    # Create the heatmap
    ax = sns.heatmap(
        heatmap_data_sorted,
        yticklabels=y_labels,
        cmap='Greys', # <-- FIX 3: Changed to black-to-white
        annot=True,  # Show the values
        fmt=".2f",  # Format values to 2 decimal places
        linewidths=.5,
        annot_kws={"size": 10},  # Smaller font for annotations
    )

    # --- 6. Style and Save ---

    plt.title(f'Top {len(top_50_df)} Terpiest {category_name.title()} Products', fontsize=18)
    plt.ylabel('Product | Brand | Profile', fontsize=16)
    plt.xticks(rotation=45, ha='right', fontsize=10)
    plt.yticks(fontsize=11)
    ax.xaxis.tick_top()  # Move X-axis labels to the top
    ax.xaxis.set_label_position('top')

    # Force a draw so matplotlib can calculate the actual rendered size
    fig.canvas.draw()
    
    # Get the renderer to calculate text bounding boxes
    renderer = fig.canvas.get_renderer()
    
    # Find the maximum label width in pixels
    max_width_pixels = 0
    for label in ax.get_yticklabels():
        bbox = label.get_window_extent(renderer=renderer)
        if bbox.width > max_width_pixels:
            max_width_pixels = bbox.width
    
    # Convert pixel width to a fraction of the total figure width
    fig_width_pixels = fig.get_window_extent().width
    new_left_margin = (max_width_pixels / fig_width_pixels)
    
    # Add a small 2% padding to the right of the text
    new_left_margin += 0.02

    # Manually adjust subplot spacing
    fig.subplots_adjust(left=new_left_margin, top=0.95, bottom=0.05, right=0.98)

    # Define the output filename
    filename = os.path.join(save_dir, f'top_50_heatmap_{category_name}.png')

    # Save the figure
    try:
        plt.savefig(filename, dpi=150)
        print(f"    SUCCESS: Saved plot to {filename}")
    except Exception as e:
        print(f"    ERROR: Failed to save plot to {filename}. Reason: {e}")

    # Close the plot to free memory
    plt.close()

def plot_dominant_terp_summary(data, category_name, save_dir):
    """
    Generates and saves the dominant terpene pie chart and top 10 lists.

    This provides a high-level overview of the terpene landscape for a product category.
    It shows which terpenes are most frequently dominant and lists the top products
    for each key terpene.

    Args:
        data (pd.DataFrame): The data for a specific product category.
        category_name (str): The name of the category.
        save_dir (str): The directory to save the plot.
    """
    print(f"  > Plotting Dominant Terp Summary for {category_name}...")

    # --- 1. Define Terpenes and Apply Filters ---

    # Define the subset of terpenes we want to analyze
    TERPS_TO_PLOT = [
        'beta-Myrcene', 'Limonene', 'beta-Caryophyllene', 'Terpinolene',
        'Linalool', 'Pinene', 'Humulene', 'alpha-Bisabolol', 'Ocimene'
    ]

    # Use the same category-specific filters as the heatmap
    filters = {
        'Flower': (
            (data['Total_Terps'] > 2) &
            (data['Total_Terps'] < 6) &
            (data['THC'] < 40) &
            (~data['Subtype'].str.contains('Infused', case=False, na=False))
        ),
        'Concentrates': (
            (data['Total_Terps'] > 5)
        ),
        'Vaporizers': (
            (data['Total_Terps'] > 5)
        )
    }

    if category_name not in filters:
        print(f"    SKIPPING: No filter logic defined for category '{category_name}'.")
        return

    df_filtered = data[filters[category_name]].copy()

    # Ensure we only have data for the terpenes we want to plot
    df_terpenes = df_filtered[TERPS_TO_PLOT].copy()

    if df_terpenes.empty:
        print(f"    SKIPPING: No products met the filter criteria for {category_name}.")
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

    # Move pie chart up to make space for the legend
    patches, texts = ax_pie.pie(
        pie_data,
        colors=pie_colors,
        startangle=90,
        center=(0.5, 0.5) # Center the pie in the axis
    )

    # Add descriptive text
    ax_pie.set_title(f'Dominant Terpene for {category_name.title()}', fontsize=36, pad=30)
    ax_pie.text(0.5, 1, 'Pie shows the % of products where a given terpene is dominant.',
                ha='center', va='center', transform=ax_pie.transAxes,
                fontsize=18, style='italic', color='#555555')


    # Add the legend below the chart
    legend_labels = [f"{name} ({perc:.1f}%)" for name, perc in zip(pie_data.index, (pie_data / pie_data.sum() * 100))]
    ax_pie.legend(patches, legend_labels, 
                  loc="upper center", # Position legend at bottom
                  bbox_to_anchor=(0.5, -0.1), # Place it below the axis
                  fontsize=36,
                  ncol=1, # Use 3 columns
                  frameon=False) # No bounding box

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
                         fontweight='bold', fontsize=24, color=color_map[terp_name])
            y_pos -= (y_step_line * 1.5) # Extra space after header

            # Draw Top 10 List
            for line in top_10_lists[terp_name]:
                ax_text.text(x_pos, y_pos, line, fontsize=12, family='monospace')
                y_pos -= y_step_line

            y_pos -= y_step_header # Extra space between lists
            current_terp_index += 1

    # --- 5. Save Figure ---

    # Define the output filename
    filename = os.path.join(save_dir, f'dominant_terp_summary_{category_name}.png')

    # Save the figure
    try:
        plt.savefig(filename, dpi=150, bbox_inches='tight')
        print(f"    SUCCESS: Saved plot to {filename}")
    except Exception as e:
        print(f"    ERROR: Failed to save plot to {filename}. Reason: {e}")

    # Close the plot to free memory
    plt.close()

def plot_value_panel_chart(data, category_name, save_dir):
    """
    Generates a 3-panel chart of the Top 25 "Best Value" products,
    showing Value Score, Price (DPG), and Total Terpenes. This allows for a
    multi-faceted view of what makes a product a good value.

    Args:
        data (pd.DataFrame): The data for the specific product category.
        category_name (str): The name of the category (e.g., 'flower').
        save_dir (str): The directory where the plot image will be saved.
    """
    print(f"  > Plotting Top 25 Value Panel Chart for {category_name}...")

    # --- 1. Define Filters and Calculate Value Score ---

    # We must have valid DPG and Total_Terps to calculate value
    df_filtered = data[
        (data['dpg'].notna()) & (data['dpg'] > 0) &
        (data['Total_Terps'].notna()) & (data['Total_Terps'] > 0)
    ].copy()

    # Calculate the "Value Score" (Terps per Dollar)
    # A higher score is better
    df_filtered['Value_Score'] = df_filtered['Total_Terps'] / df_filtered['dpg']

    # --- 2. Get Top 25 Unique Products ---

    # FIX 2: Find top 25 *unique* products based on Name_Clean
    # This prevents the "multiple bar" issue.
    df_unique = df_filtered.drop_duplicates('Name_Clean')
    df_top25 = df_unique.nlargest(25, 'Value_Score')

    if df_top25.empty:
        print(f"    SKIPPING: No products with valid Value Score for {category_name}.")
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

    sns.set_style("white")

    # Create a figure with 3 subplots that share a Y-axis
    fig, (ax1, ax2, ax3) = plt.subplots(
        1, 3,
        figsize=(20, 14),
        sharey=True # This links all three Y-axes
    )

    # Set a main title for the entire figure
    fig.suptitle(f'Top 25 "High Terps & Low Dollars Per Gram" Products: {category_name.title()}',
                 fontsize=26, y=0.98)
    
    # --- FIX 1 & 3: Define Custom Gray-to-Hue Colormaps ---
    
    # Base gray color
    gray = (0.5, 0.5, 0.5)
    
    # Panel 1: Gray to Green (for Value Score)
    cmap1 = LinearSegmentedColormap.from_list('gray_to_green', 
                                              [gray, sns.color_palette('Greens')[-1]])
    norm1 = mcolors.Normalize(vmin=df_top25['Value_Score'].min(), 
                              vmax=df_top25['Value_Score'].max())
    colors1 = [cmap1(norm1(val)) for val in df_top25['Value_Score']]
    
    # Panel 2: Red (Low DPG) to Gray (High DPG)
    cmap2 = LinearSegmentedColormap.from_list('red_to_gray', 
                                              [sns.color_palette('Reds')[-1], gray])
    norm2 = mcolors.Normalize(vmin=df_top25['dpg'].min(), 
                              vmax=df_top25['dpg'].max())
    colors2 = [cmap2(norm2(val)) for val in df_top25['dpg']]
    
    # Panel 3: Gray to Blue (for Total Terps)
    cmap3 = LinearSegmentedColormap.from_list('gray_to_blue', 
                                              [gray, sns.color_palette('Blues')[-1]])
    norm3 = mcolors.Normalize(vmin=df_top25['Total_Terps'].min(), 
                              vmax=df_top25['Total_Terps'].max())
    colors3 = [cmap3(norm3(val)) for val in df_top25['Total_Terps']]
    

    # --- Plot 1: Value Score (Terps per Dollar) ---
    bars1 = ax1.barh(y_labels, df_top25['Value_Score'],
                     color=colors1) # Use value-mapped colors
    # FIX 4: Update x-label
    ax1.set_xlabel('Value Score (Terps / DPG)', fontsize=22)
    ax1.bar_label(bars1, fmt='%.2f', label_type='center', 
                  color='white', fontweight='bold', padding=2, fontsize=18)

    # --- Plot 2: Price per Gram (DPG) ---
    bars2 = ax2.barh(y_labels, df_top25['dpg'],
                     color=colors2) # Use value-mapped colors
    ax2.set_xlabel('Price per Gram (DPG $)', fontsize=22)
    ax2.bar_label(bars2, fmt='$%.0f', label_type='center', 
                  color='white', fontweight='bold', padding=2, fontsize=18)

    # --- Plot 3: Total Terpenes ---
    bars3 = ax3.barh(y_labels, df_top25['Total_Terps'],
                     color=colors3) # Use value-mapped colors
    ax3.set_xlabel('Total Terpenes (%)', fontsize=22)
    ax3.bar_label(bars3, fmt='%.1f%%', label_type='center', 
                  color='white', fontweight='bold', padding=2, fontsize=18)

    # --- 5. Style and Save ---

    # FIX 2: Remove all axis ticks, tick labels, and spines
    for ax in [ax1, ax2, ax3]:
        ax.set_xticks([])
        ax.set_xticklabels([])
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['bottom'].set_visible(False)
        ax.spines['left'].set_visible(False)
    
    # Keep the y-tick LABELS but hide the ticks
    ax1.tick_params(axis='y', length=0)
    
    # FIX 3: Color the y-axis labels to match the Value Score
    for label, color in zip(ax1.get_yticklabels(), colors1):
        label.set_color(color)
        
    # FIX 4: Set Y-axis label font size
    ax1.tick_params(axis='y', labelsize=16)

    # Force a draw so matplotlib can calculate the actual rendered size of labels
    fig.canvas.draw()
    
    # Get the renderer to calculate text bounding boxes
    renderer = fig.canvas.get_renderer()
    
    # Find the maximum label width in pixels
    max_width_pixels = 0
    for label in ax1.get_yticklabels():
        bbox = label.get_window_extent(renderer=renderer)
        if bbox.width > max_width_pixels:
            max_width_pixels = bbox.width
    
    # Convert pixel width to a fraction of the total figure width
    fig_width_pixels = fig.get_window_extent().width
    new_left_margin = (max_width_pixels / fig_width_pixels)
    
    # Add a small 2% padding to the right of the text
    new_left_margin += 0.02

    # Manually adjust subplot spacing
    fig.subplots_adjust(left=new_left_margin, top=0.95, bottom=0.05, right=0.98, wspace=0.35)

    # Define the output filename
    filename = os.path.join(save_dir, f'top_25_value_panel_{category_name}.png')

    # Save the figure
    try:
        plt.savefig(filename, dpi=150)
        print(f"    SUCCESS: Saved plot to {filename}")
    except Exception as e:
        print(f"    ERROR: Failed to save plot to {filename}. Reason: {e}")
    
    # Close the plot to free memory
    plt.close()
