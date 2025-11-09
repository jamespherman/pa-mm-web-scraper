# scrapers/scraper_utils.py
#
# This file holds helper functions that are used by
# multiple scrapers, like standardizing weights, cleaning text,
# or parsing lab data.

import re

# --- Master Standardization Maps ---

# This map is the single source of truth for standardizing terpene names.
# It accounts for variations in capitalization, spacing, and common abbreviations
# found across all scraper sources. The key is the raw, lowercase name, and the
# value is the canonical name we want in the final dataset.
MASTER_TERPENE_MAP = {
    # --- Alpha-Pinene ---
    'alpha-pinene': 'alpha-Pinene', 'a-pinene': 'alpha-Pinene', 'alphapinene': 'alpha-Pinene', 'alpha_pinene': 'alpha-Pinene', 'pinene': 'alpha-Pinene',
    # --- Beta-Pinene ---
    'beta-pinene': 'beta-Pinene', 'b-pinene': 'beta-Pinene', 'betapinene': 'beta-Pinene', 'beta_pinene': 'beta-Pinene',
    # --- Beta-Myrcene ---
    'beta-myrcene': 'beta-Myrcene', 'myrcene': 'beta-Myrcene', 'b-myrcene': 'beta-Myrcene', 'betamyrcene': 'beta-Myrcene', 'beta_myrcene': 'beta-Myrcene',
    # --- Limonene ---
    'limonene': 'Limonene', 'd-limonene': 'Limonene',
    # --- Beta-Caryophyllene ---
    'beta-caryophyllene': 'beta-Caryophyllene', 'caryophyllene': 'beta-Caryophyllene', 'b-caryophyllene': 'beta-Caryophyllene', 'betacaryophyllene': 'beta-Caryophyllene', 'beta_caryophyllene': 'beta-Caryophyllene',
    # --- Linalool ---
    'linalool': 'Linalool',
    # --- Terpinolene ---
    'terpinolene': 'Terpinolene',
    # --- Humulene ---
    'humulene': 'Humulene', 'alpha-humulene': 'Humulene', 'a-humulene': 'Humulene',
    # --- Ocimene ---
    'ocimene': 'Ocimene', 'beta-ocimene': 'Ocimene', 'b-ocimene': 'Ocimene',
    # --- Guaiol ---
    'guaiol': 'Guaiol',
    # --- Alpha-Bisabolol ---
    'alpha-bisabolol': 'alpha-Bisabolol', 'bisabolol': 'alpha-Bisabolol', 'a-bisabolol': 'alpha-Bisabolol', 'alphabisabolol': 'alpha-Bisabolol',
    # --- Camphene ---
    'camphene': 'Camphene',
    # --- Caryophyllene Oxide ---
    'caryophyllene oxide': 'Caryophyllene Oxide', 'caryophylleneoxide': 'Caryophyllene Oxide', 'caryophyllene_oxide': 'Caryophyllene Oxide',
}

# This map standardizes the main product categories.
MASTER_CATEGORY_MAP = {
    'vape': 'vaporizers',
    'vapes': 'vaporizers',
    'concentrate': 'concentrates',
    'flower': 'flower',
    # Add other mappings as discovered from schema reports
}

# This map standardizes product subtypes. (Optional but recommended)
MASTER_SUBCATEGORY_MAP = {
    'cartridge': 'Cartridges',
    'cartridges': 'Cartridges',
    'disposable': 'Disposables',
    'disposables': 'Disposables',
    # Add other mappings
}

# This map consolidates different brand names and variations into a single,
# canonical brand. This is moved directly from the old `analysis.py` module.
brand_map = {
    # --- GTI ---
    'Good Green': 'GTI', '&Shine': 'GTI', 'Rythm': 'GTI', 'Rhythm': 'GTI',
    # --- Jushi ---
    'The Bank': 'Jushi', 'The Lab': 'Jushi', 'Seche': 'Jushi', 'Lab': 'Jushi',
    # --- Trulieve ---
    'TruFlower': 'Trulieve', 'Cultivar Collection': 'Trulieve',
    'Modern Flower': 'Trulieve', 'Avenue': 'Trulieve', 'Muse': 'Trulieve',
    'Moxie': 'Trulieve', 'Franklin Labs': 'Trulieve',
    'Khalifa Kush': 'Trulieve', 'Roll One (Trulieve)': 'Trulieve',
    # --- Ayr ---
    'Lost In Translation': 'Ayr', 'Revel': 'Ayr', 'Origyn': 'Ayr',
    'Seven Hills': 'Ayr', 'Kynd': 'Ayr',
    # --- Cresco ---
    'Supply/Cresco': 'Cresco', 'FloraCal': 'Cresco',
    'Cresco Labs': 'Cresco', 'Sunnyside': 'Cresco',
    # --- Curaleaf ---
    'Grass Roots': 'Curaleaf', 'Blades': 'Curaleaf', 'Select': 'Curaleaf', 'Select Briq': 'Curaleaf',
    # --- Verano ---
    'Essence': 'Verano', 'Savvy': 'Verano', 'Muv': 'Verano',
    # --- Vytal ---
    'Vytal Options': 'Vytal',
    'Solventless by Vytal': 'Vytal Solventless', # Kept separate per user
    'mood by Vytal': 'mood', # Mapped to 'mood' per user
    # --- R.O. ---
    'R.O. Ground': 'R.O.', 'R.O. Shake': 'R.O.',
    # --- Strane ---
    'Strane Stash': 'Strane', 'Strane Reserve': 'Strane',
    # --- Misc Single-Rule Consolidations ---
    'The Woods': 'Terrapin',
    'Cookies': 'Kind Tree', 'Gage': 'Kind Tree',
    'Standard Farms': 'Standard Farms', 'Old Pal': 'Standard Farms', 'Highsman': 'Standard Farms',
    'Tyson 2.0': 'Columbia Care', 'Triple Seven': 'Columbia Care', 'Classix': 'Columbia Care',
    'FarmaceuticalRx': 'FRX',
    'Maitri Medicinals': 'Maitri',
    'Maitri Genetics': 'Maitri',
    'Natural Selections': 'Natural Selections',
    'Organic Remedies': 'Organic Remedies',
    'Penn Health Group': 'PHG',
    'Penn Health': 'PHG',
    'Prime Wellness': 'Prime',
    'SupplyTM': 'Supply', 'Supply TM': 'Supply',
    'Calypso Bountiful': 'Calypso',
    'Garcia Hand Picked': 'Garcia',
    'Redemption Shake': 'Redemption',
    'Sunshine Cannabis': 'Sunshine',
    'Ozone Reserve': 'Ozone',
}


def convert_to_grams(weight_str):
    """
    Converts a weight string (e.g., '1g', '1/8oz', '3.5g')
    to a float representing the weight in grams. It handles a variety of
    common formats, including fractions, ounces, and grams.

    Args:
        weight_str (str): The string representation of the weight.

    Returns:
        float: The weight in grams, or None if the format is not recognized.
    """
    if not isinstance(weight_str, str):
        return None

    weight_str = weight_str.lower().strip()

    # Dictionary for direct string-to-gram conversions
    # We are standardizing on 3.5g per eighth.
    weight_map = {
        'gram': 1.0, '1g': 1.0, '1 g': 1.0, '1gc': 1.0, 'two gram': 2.0, '2g': 2.0,
        '2 g': 2.0, '2gc': 2.0, '3gc': 3.0, 'half gram': 0.5, '0.5g': 0.5, '0.5 g': 0.5,
        'eighth ounce': 3.5, 'eighth_ounce': 3.5, '1/8oz': 3.5, '1/8 oz': 3.5, '3.5g': 3.5,
        '3.5 g': 3.5, '3.5gc': 3.5, 'quarter ounce': 7.0, 'quarter_ounce': 7.0, '1/4oz': 7.0,
        '1/4 oz': 7.0, '7g': 7.0, '7 g': 7.0, 'half ounce': 14.0, '1/2oz': 14.0,
        '1/2 oz': 14.0, '14g': 14.0, '14 g': 14.0, 'ounce': 28.0, '1oz': 28.0, '1 oz': 28.0,
        '28g': 28.0, '28 g': 28.0,
    }

    if weight_str in weight_map:
        return weight_map[weight_str]

    # Fallback to regex for patterns like '5g' or '500mg'
    match_g = re.match(r'([\d\.]+)\s*(g|gram|grams)', weight_str)
    if match_g:
        return float(match_g.group(1))

    match_mg = re.match(r'([\d\.]+)\s*mg', weight_str)
    if match_mg:
        return float(match_mg.group(1)) / 1000.0
    
    match_oz = re.match(r'([\d\.]+)\s*(oz|ounce|ounces)', weight_str)
    if match_oz:
        return float(match_oz.group(1)) * 28.0

    return None
