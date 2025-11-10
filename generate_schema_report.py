
import json
import sys
import os
import glob
from collections import defaultdict, Counter

def main():
    if len(sys.argv) != 2:
        print("Usage: python generate_schema_report.py <path_to_raw_json_file>")
        sys.exit(1)

    input_filepath = sys.argv[1]
    if not os.path.exists(input_filepath):
        print(f"Error: File not found at {input_filepath}")
        sys.exit(1)

    # Create output directory if it doesn't exist
    output_dir = "schema_reports"
    os.makedirs(output_dir, exist_ok=True)

    # Determine output filename
    base_filename = os.path.basename(input_filepath)
    output_filename = os.path.splitext(base_filename)[0].replace("_raw_products", "") + "_schema_report.md"
    output_filepath = os.path.join(output_dir, output_filename)

    # Load the raw JSON data
    with open(input_filepath, 'r') as f:
        data = json.load(f)

    # Find the main list of products
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
        # We can still generate an empty report or just exit
        with open(output_filepath, 'w') as f:
            f.write("# Data Dictionary\n\n")
            f.write(f"Source File: `{base_filename}`\n\n")
            f.write("No products found to analyze.\n")
        sys.exit(0)
    
    # --- Core Profiling Logic ---
    schema_tree = defaultdict(lambda: {
        "_count": 0,
        "_types": set(),
        "_values": set(),
        "_range": [float('inf'), float('-inf')]
    })

    # Build the schema by iterating through each product
    schema_tree = {} # Using a normal dict for the root
    for product in products:
        discover_schema(product, schema_tree)

def discover_schema(data, schema_node):
    """Recursively traverses the data to build the schema tree."""
    if isinstance(data, dict):
        for key, value in data.items():
            # Ensure the node for the key exists
            node = schema_node.setdefault(key, {
                "_count": 0,
                "_types": set(),
                "_values": set(),
                "_range": [float('inf'), float('-inf')]
            })
            node["_count"] += 1
            node["_types"].add(type(value).__name__)

            # Handle value and range tracking
            if isinstance(value, (int, float)) and value is not None:
                node["_range"][0] = min(node["_range"][0], value)
                node["_range"][1] = max(node["_range"][1], value)
            elif isinstance(value, str) and len(value) <= 30 and len(node["_values"]) < 50:
                node["_values"].add(value)
            
            # Recurse for nested structures
            if isinstance(value, dict):
                discover_schema(value, node)
            elif isinstance(value, list):
                # We treat all items in a list as having the same schema
                # under a generic "_items_" key.
                list_node = node.setdefault("_items_", defaultdict(lambda: {
                    "_count": 0,
                    "_types": set(),
                    "_values": set(),
                    "_range": [float('inf'), float('-inf')]
                }))
                for item in value:
                    discover_schema(item, list_node)

    elif isinstance(data, list):
        # This case handles if the top-level product itself is a list,
        # which is less common but possible.
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
