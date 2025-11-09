import re
import numpy as np

def convert_to_grams(weight_str):
    """
    Converts a string like "3.5g", "500mg", or "1/8oz" into grams.
    """
    if not isinstance(weight_str, str):
        return np.nan

    weight_str_lower = weight_str.lower().strip()

    # Regex to find numbers (including fractions)
    match = re.search(r'([\d\./]+)', weight_str_lower)
    if not match:
        return np.nan

    value_str = match.group(1)
    value = 0

    # Handle fractions like "1/8"
    if '/' in value_str:
        parts = value_str.split('/')
        if len(parts) == 2 and parts[0].replace('.', '', 1).isdigit() and parts[1].replace('.', '', 1).isdigit():
            try:
                value = float(parts[0]) / float(parts[1])
            except ZeroDivisionError:
                return np.nan
        else:
            return np.nan
    # Handle decimal numbers
    elif value_str.replace('.', '', 1).isdigit():
        value = float(value_str)
    else:
        return np.nan

    # Unit conversion
    if 'mg' in weight_str_lower:
        return value / 1000.0  # Milligrams to Grams
    if 'oz' in weight_str_lower:
        return value * 28.3495  # Ounces to Grams
    if 'g' in weight_str_lower:
        return value  # Already in Grams
    if 'ml' in weight_str_lower:
        return value  # Assume 1ml = 1g for our purposes
    
    # If no unit is found but it's a number, it's ambiguous.
    # But for menu items, "1" or "0.5" often implies grams.
    # Let's assume grams if no unit is found
    return value