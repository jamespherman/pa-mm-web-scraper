import json

def load_products_from_file(filename="all_products.json"):
    """Loads the product data from the specified JSON file."""
    try:
        with open(filename, "r", encoding="utf-8") as f:
            print(f"Loading data from {filename}...")
            products = json.load(f)
            print(f"Successfully loaded {len(products)} products.")
            return products
    except FileNotFoundError:
        print(f"Error: {filename} not found.")
        print("Please run the 'scrape_all_products.py' script first.")
        return None
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from {filename}.")
        return None

def parse_product_data(products):
    """
    Loops through products and prints the info we originally wanted.
    """
    if not products:
        print("No products to parse.")
        return

    # Let's just look at the first 10 products
    for product in products[:10]:
        name = product.get("Name", "N/A")
        brand = product.get("brandName", "N/A")
        product_type = product.get("type", "N/A")
        
        # Prices are in a list, e.g., [43]. Get the first one.
        price = product.get("Prices", [None])[0]

        print("---------------------------------")
        print(f"Name:    {name}")
        print(f"Brand:   {brand}")
        print(f"Type:    {product_type}")
        print(f"Price:   ${price}")

        # Now, let's find the specific cannabinoids
        cannabinoids = product.get("cannabinoidsV2", [])
        if cannabinoids:
            print("Cannabinoids:")
            
            # Loop through the list of cannabinoid objects
            for c in cannabinoids:
                c_name = c.get("cannabinoid", {}).get("name", "Unknown")
                c_value = c.get("value", 0)
                c_unit = c.get("unit", "")

                # Let's just print a few key ones
                if "THCA" in c_name:
                    print(f"  - THCA:  {c_value}{'%' if c_unit == 'PERCENTAGE' else ''}")
                if "TAC" in c_name:
                    print(f"  - TAC:   {c_value}{'%' if c_unit == 'PERCENTAGE' else ''}")
        
        print("---------------------------------\n")

# --- Main execution ---
if __name__ == "__main__":
    all_products = load_products_from_file()
    parse_product_data(all_products)