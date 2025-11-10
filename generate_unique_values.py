import json
import os
import glob
import re

# Global sets to store all unique values
all_brands = set()
all_categories = set()
all_subcategories = set()
all_compound_names = set()

def parse_dutchie_data(data, sets):
    """Parses Dutchie data to extract unique values."""
    products = data
    for product in products:
        if brand := product.get('brandName'):
            sets['brands'].add(brand.strip())
        if category := product.get('type'):
            sets['categories'].add(category.strip())
        if subcategory := product.get('subcategory'):
            sets['subcategories'].add(subcategory.strip())
        for item in product.get('terpenes', []) or []:
            if name := item.get('libraryTerpene', {}).get('name'):
                sets['compounds'].add(name.strip())
        for item in product.get('cannabinoidsV2', []) or []:
            if name := item.get('cannabinoid', {}).get('name'):
                sets['compounds'].add(name.strip())

def parse_cresco_data(data, sets):
    """Parses Cresco data to extract unique values."""
    products = data
    EXCLUDE_KEYS = ['id', 'location_id', 'package_number', 'total_terps', 'thc_total', 'cbd_total']
    for product in products:
        if brand := product.get('brand'):
            sets['brands'].add(brand.strip())
        if category := product.get('sku', {}).get('product', {}).get('category'):
            sets['categories'].add(category.strip())
        if subcategory := product.get('sku', {}).get('product', {}).get('sub_category'):
            sets['subcategories'].add(subcategory.strip())
        if potency := product.get('potency', {}):
            for key in potency.keys():
                if key not in EXCLUDE_KEYS:
                    sets['compounds'].add(key.strip())

def parse_trulieve_data(data, sets):
    """Parses Trulieve data to extract unique values."""
    products = data

    # Manually add THC and CBD
    sets['compounds'].add("THC")
    sets['compounds'].add("CBD")

    for product in products:
        if brand := product.get('brand'):
            sets['brands'].add(brand.strip())
        if category := product.get('category'):
            sets['categories'].add(category.strip())
        if subcategory := product.get('subcategory'):
            sets['subcategories'].add(subcategory.strip())

        for item in product.get('terpenes', []) or []:
            if name := item.get('name'):
                sets['compounds'].add(name.strip())
        for item in product.get('cannabinoids', []) or []:
            if name := item.get('name'):
                sets['compounds'].add(name.strip())

def parse_iheartjane_data(data, sets):
    """Parses iHeartJane data to extract unique values."""
    products = data
    for product in products:
        attrs = product.get('search_attributes', {})
        if brand := attrs.get('brand'):
            sets['brands'].add(brand.strip())
        if category := attrs.get('kind'):
            sets['categories'].add(category.strip())
        if subcategory := attrs.get('kind_subtype'):
            sets['subcategories'].add(subcategory.strip())

        # Structured lab results
        for result in attrs.get('lab_results', []) or []:
            if 'lab_results' in result and isinstance(result['lab_results'], list):
                for sub_result in result['lab_results']:
                    if name := sub_result.get('compound_name'):
                        sets['compounds'].add(name.strip())

        # Fallback 1: compound_names list
        for name in attrs.get('compound_names', []) or []:
            if name:
                sets['compounds'].add(name.strip())

        # Fallback 2: Unstructured store_notes
        if store_notes := attrs.get('store_notes', ''):
            matches = re.findall(r"([a-zA-Z\s-]+)[\s:]*([\d\.]+)%", store_notes)
            for name, value in matches:
                if name:
                    sets['compounds'].add(name.strip())

def main():
    """Main function to process all raw data files and generate the report."""

    sets = {
        'brands': all_brands,
        'categories': all_categories,
        'subcategories': all_subcategories,
        'compounds': all_compound_names
    }

    for filepath in glob.glob('raw_data/*.json'):
        with open(filepath, 'r') as f:
            data = json.load(f)

        filename = os.path.basename(filepath)

        if "dutchie" in filename:
            parse_dutchie_data(data, sets)
        elif "cresco" in filename:
            parse_cresco_data(data, sets)
        elif "trulieve" in filename:
            parse_trulieve_data(data, sets)
        elif "iheartjane" in filename:
            parse_iheartjane_data(data, sets)

    with open('UNIQUE_VALUE_REPORT.md', 'w') as f:
        f.write("# Unique Value Report\n\n")

        f.write("## Brands\n")
        for brand in sorted(list(all_brands)):
            f.write(f"- {brand}\n")

        f.write("\n## Categories\n")
        for category in sorted(list(all_categories)):
            f.write(f"- {category}\n")

        f.write("\n## Subcategories\n")
        for subcategory in sorted(list(all_subcategories)):
            f.write(f"- {subcategory}\n")

        f.write("\n## Compound Names\n")
        for compound in sorted(list(all_compound_names)):
            f.write(f"- {compound}\n")

if __name__ == "__main__":
    main()
