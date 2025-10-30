# scrapers/scraper_utils.py
#
# This file will hold helper functions that are used by
# multiple scrapers, like standardizing weights, cleaning text,
# or parsing lab data.

import re

def convert_to_grams(weight_str):
    """
    Converts a weight string (e.g., '1g', '1/8oz', '3.5g')
    to a float representing the weight in grams.
    """
    if not isinstance(weight_str, str):
        return None

    weight_str = weight_str.lower().strip()

    weight_map = {
        'gram': 1.0,
        '1g': 1.0,
        'half gram': 0.5,
        '0.5g': 0.5,
        'eighth ounce': 3.5,
        '3.5g': 3.5,
        'quarter ounce': 7.0,
        '7g': 7.0,
        'half ounce': 14.0,
        '14g': 14.0,
        'ounce': 28.0,
        '28g': 28.0,
        # also handle cases from the original function
        '1/8oz': 3.5,
        '1/8 oz': 3.5,
        '3.5 g': 3.5,
        '1/4oz': 7.0,
        '1/4 oz': 7.0,
        '7 g': 7.0,
        '1/2oz': 14.0,
        '1/2 oz': 14.0,
        '14 g': 14.0,
        '1oz': 28.0,
        '1 oz': 28.0,
        '28 g': 28.0,
        '0.5 g': 0.5,
    }

    if weight_str in weight_map:
        return weight_map[weight_str]

    # Handle simple 'g' and 'mg' from the original function
    match_g = re.match(r'([\d\.]+)\s*g', weight_str)
    if match_g:
        return float(match_g.group(1))
        
    match_mg = re.match(r'([\d\.]+)\s*mg', weight_str)
    if match_mg:
        return float(match_mg.group(1)) / 1000.0

    return None