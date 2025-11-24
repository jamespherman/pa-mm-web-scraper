# utils/generate_strain_map.py
import argparse
import pandas as pd
import glob
import os
import difflib
import re

def get_latest_csv_file(pattern='PA_Scraped_Data_*.csv'):
    """
    Finds the latest CSV file matching the pattern in the current directory.
    """
    files = glob.glob(pattern)
    if not files:
        return None
    # Sort by modification time, newest first
    files.sort(key=os.path.getmtime, reverse=True)
    return files[0]

def calculate_similarity(s1, s2):
    """
    Calculates the similarity ratio between two strings.
    """
    return difflib.SequenceMatcher(None, s1, s2).ratio()

def generate_map(file_path):
    """
    Generates a strain map dictionary based on fuzzy duplicates found in the CSV.
    """
    print(f"Loading data from: {file_path}")
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"Error loading CSV: {e}")
        return

    required_cols = ['Brand', 'Name_Clean']
    for col in required_cols:
        if col not in df.columns:
            print(f"Error: Missing required column '{col}' in CSV.")
            return

    # Filter out empty names
    df = df.dropna(subset=['Brand', 'Name_Clean'])

    strain_map = {}

    # Group by Brand
    grouped = df.groupby('Brand')

    print("\nAnalyzing brands for fuzzy duplicates...")

    for brand, group in grouped:
        names = group['Name_Clean'].unique()
        names = sorted([str(n) for n in names if n]) # Ensure strings

        if len(names) < 2:
            continue

        # Compare every name with every other name in the brand
        # We use a visited set to avoid redundant checks
        visited = set()

        for i in range(len(names)):
            name1 = names[i]
            if name1 in visited:
                continue

            for j in range(i + 1, len(names)):
                name2 = names[j]
                if name2 in visited:
                    continue

                similarity = calculate_similarity(name1.lower(), name2.lower())

                if similarity > 0.85:
                    print(f"  [{brand}] Match found: '{name1}' <-> '{name2}' ({similarity:.2f})")

                    # Determine which is the "canonical" name.
                    # Heuristic: Shorter name is usually the base strain (e.g. "GMO" vs "GMO Popcorn")
                    # But also check length. If lengths are similar, maybe use the one that appears more often?
                    # For now, let's use the shorter one as canonical.

                    if len(name1) <= len(name2):
                        short = name1
                        long_ver = name2
                    else:
                        short = name2
                        long_ver = name1

                    # Add to map: long -> short
                    strain_map[long_ver] = short

                    # Mark as visited so we don't map them again in this loop
                    # (Note: this is a simplification, might miss some relations if A~B and B~C but A!~C)
                    # For generating a starting map, this is acceptable.
                    # We don't mark 'short' as visited because it might match others.
                    visited.add(long_ver)

    print("\n--- Generated Strain Map ---")
    print("# Copy this dictionary into 'scraper_utils.py' or equivalent.")
    print("STRAIN_MAP = {")
    for key in sorted(strain_map.keys()):
        val = strain_map[key]
        print(f"    '{key}': '{val}',")
    print("}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate a strain map from fuzzy duplicates.")
    parser.add_argument("file", nargs="?", help="Path to the CSV file to analyze.")

    args = parser.parse_args()

    target_file = args.file

    if not target_file:
        print("No file specified. Looking for latest 'PA_Scraped_Data_*.csv'...")
        target_file = get_latest_csv_file()

    if target_file:
        generate_map(target_file)
    else:
        print("Error: No input file found. Please provide a CSV file path.")
