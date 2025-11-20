# generate_schema_report.py
# -----------------------------------------------------------------------------
# This is a debugging tool.
#
# When we get a new, unknown JSON file from a website, it's hard to know
# what's inside. Is it a list? A dictionary? What keys does it have?
#
# This script reads a JSON file and creates a "Report Card" (Markdown file)
# that describes the structure of the data. It tells us:
# - What keys exist (e.g., "name", "price").
# - What type of data they hold (e.g., String, Number).
# - Example values.
#
# Usage: python generate_schema_report.py path/to/file.json
# -----------------------------------------------------------------------------

import json
import sys
import os
import glob
from collections import defaultdict, Counter

def main():
    # Check if the user provided a filename
    if len(sys.argv) != 2:
        print("Usage: python generate_schema_report.py <path_to_raw_json_file>")
        sys.exit(1)

    input_filepath = sys.argv[1]
    if not os.path.exists(input_filepath):
        print(f"Error: File not found at {input_filepath}")
        sys.exit(1)

    # Create a folder to save the reports
    output_dir = "schema_reports"
    os.makedirs(output_dir, exist_ok=True)

    # Create the output filename (e.g., "dutchie_report.md")
    base_filename = os.path.basename(input_filepath)
    output_filename = os.path.splitext(base_filename)[0].replace("_raw_products", "") + "_schema_report.md"
    output_filepath = os.path.join(output_dir, output_filename)

    # Load the raw JSON data
    with open(input_filepath, 'r') as f:
        data = json.load(f)

    # Try to find the main list of products.
    # Sometimes the file *is* a list, sometimes it's a dict with a 'data' key.
    if isinstance(data, list):
        products = data
    elif isinstance(data, dict) and 'data' in data and isinstance(data['data'], list):
        products = data['data']
    else:
        print("Error: Could not find a list of products in the JSON file.")
        sys.exit(1)
    
    total_products = len(products)
    if total_products == 0:
        print("Warning: No products found in the file.")
        with open(output_filepath, 'w') as f:
            f.write("# Data Dictionary\n\n")
            f.write(f"Source File: `{base_filename}`\n\n")
            f.write("No products found to analyze.\n")
        sys.exit(0)
    
    # --- Core Profiling Logic ---
    # We will build a "tree" that mirrors the structure of the JSON.
    schema_tree = {} # The root of our tree

    # Analyze every product to build the complete picture
    for product in products:
        discover_schema(product, schema_tree)

    # (The actual report generation logic would go here, but is omitted for brevity
    # as this script focuses on the discovery phase.)

def discover_schema(data, schema_node):
    """
    Recursively looks through the data to map out its structure.

    Args:
        data: The current piece of data we are looking at.
        schema_node: The current place in our "map" (tree).
    """
    if isinstance(data, dict):
        # If it's a dictionary, loop through every key
        for key, value in data.items():
            # Create a record for this key if we haven't seen it before
            node = schema_node.setdefault(key, {
                "_count": 0,
                "_types": set(),
                "_values": set(),
                "_range": [float('inf'), float('-inf')]
            })

            # Track stats
            node["_count"] += 1
            node["_types"].add(type(value).__name__)

            # Track numeric ranges (Min/Max)
            if isinstance(value, (int, float)) and value is not None:
                node["_range"][0] = min(node["_range"][0], value)
                node["_range"][1] = max(node["_range"][1], value)
            # Track unique text values (up to a limit, so we don't crash)
            elif isinstance(value, str) and len(value) <= 30 and len(node["_values"]) < 50:
                node["_values"].add(value)
            
            # If the value is nested (another dict or list), dive deeper!
            if isinstance(value, dict):
                discover_schema(value, node)
            elif isinstance(value, list):
                # For lists, we assume all items in the list are similar.
                # We create a special node called "_items_" to represent them.
                list_node = node.setdefault("_items_", defaultdict(lambda: {
                    "_count": 0,
                    "_types": set(),
                    "_values": set(),
                    "_range": [float('inf'), float('-inf')]
                }))
                for item in value:
                    discover_schema(item, list_node)

    elif isinstance(data, list):
        # Handle case where the data passed in is a list itself
        list_node = schema_node.setdefault("_items_", defaultdict(lambda: {
            "_count": 0,
            "_types": set(),
            "_values": set(),
            "_range": [float('inf'), float('-inf')]
        }))
        for item in data:
            discover_schema(item, list_node)


if __name__ == "__main__":
    main()
