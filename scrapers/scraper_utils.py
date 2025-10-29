# scrapers/scraper_utils.py
#
# This file will hold helper functions that are used by
# multiple scrapers, like standardizing weights, cleaning text,
# or parsing lab data.

import re

def convert_to_grams(weight_str):
    """
    Migrated from the MATLAB 'convertToGrams' function.
    Takes a weight string (e.g., '1g', '1/8oz', '3.5g')
    and returns the weight in grams as a float.
    """
    if not isinstance(weight_str, str):
        return None

    weight_str = weight_str.lower().strip()
    
    # Handle specific cases first
    if weight_str in ['1/8oz', 'eighth ounce', '1/8 oz', '3.5g', '3.5 g']:
        return 3.5
    if weight_str in ['1/4oz', 'quarter ounce', '1/4 oz', '7g', '7 g']:
        return 7.0
    if weight_str in ['1/2oz', 'half ounce', '1/2 oz', '14g', '14 g']:
        return 14.0
    if weight_str in ['1oz', 'ounce', '1 oz', '28g', '28 g']:
        return 28.0
    
    # Handle simple 'g' and 'mg'
    match_g = re.match(r'([\d\.]+)\s*g', weight_str)
    if match_g:
        return float(match_g.group(1))
        
    match_mg = re.match(r'([\d\.]+)\s*mg', weight_str)
    if match_mg:
        return float(match_mg.group(1)) / 1000.0
        
    # Handle 'half gram'
    if weight_str in ['half gram', '0.5g', '0.5 g']:
        return 0.5
        
    return None # Return None if no match