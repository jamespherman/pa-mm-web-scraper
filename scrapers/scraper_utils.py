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

    # Dictionary for direct string-to-gram conversions
    # We are standardizing on 3.5g per eighth.
    weight_map = {
        # Grams
        'gram': 1.0,
        '1g': 1.0,
        '1 g': 1.0,
        '1gc': 1.0,
        'two gram': 2.0,
        '2g': 2.0,
        '2 g': 2.0,
        '2gc': 2.0,
        '3gc': 3.0,
        # Half Gram
        'half gram': 0.5,
        '0.5g': 0.5,
        '0.5 g': 0.5,
        # Eighth Ounce (3.5g)
        'eighth ounce': 3.5,
        '1/8oz': 3.5,
        '1/8 oz': 3.5,
        '3.5g': 3.5,
        '3.5 g': 3.5,
        '3.5gc': 3.5,
        # Quarter Ounce (7g)
        'quarter ounce': 7.0,
        '1/4oz': 7.0,
        '1/4 oz': 7.0,
        '7g': 7.0,
        '7 g': 7.0,
        # Half Ounce (14g)
        'half ounce': 14.0,
        '1/2oz': 14.0,
        '1/2 oz': 14.0,
        '14g': 14.0,
        '14 g': 14.0,
        # Ounce (28g)
        'ounce': 28.0,
        '1oz': 28.0,
        '1 oz': 28.0,
        '28g': 28.0,
        '28 g': 28.0,
    }

    if weight_str in weight_map:
        return weight_map[weight_str]

    # Fallback to regex for patterns like '5g' or '500mg'
    # Match g/gram/grams
    match_g = re.match(r'([\d\.]+)\s*(g|gram|grams)', weight_str)
    if match_g:
        return float(match_g.group(1))

    # Match mg
    match_mg = re.match(r'([\d\.]+)\s*mg', weight_str)
    if match_mg:
        return float(match_mg.group(1)) / 1000.0
    
    # Match oz
    match_oz = re.match(r'([\d\.]+)\s*(oz|ounce|ounces)', weight_str)
    if match_oz:
        return float(match_oz.group(1)) * 28.0 # Standardizing on 28g/oz

    return None