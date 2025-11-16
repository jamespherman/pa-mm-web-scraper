# scrapers/scraper_utils.py
#
# This file holds helper functions that are used by
# multiple scrapers, like standardizing weights, cleaning text,
# or parsing lab data.

import re

# --- Master Standardization Maps ---

BRAND_MAP = {
    # Confirmed Brands
    "& Shine": "&Shine", "&Shine": "&Shine", "Cresco": "Cresco", "Cresco™": "Cresco",
    "Doctor Solomon's": "Doctor Solomon's", "Dr. Solomon's": "Doctor Solomon's",
    "FIND": "Find", "Find": "Find", "Find.": "Find", "FloraCal": "FloraCal Farms",
    "FloraCal Farms": "FloraCal Farms", "Garcia": "Garcia Hand Picked",
    "Garcia Hand Picked": "Garcia Hand Picked", "Maitri": "Maitri",
    "Maitri Genetics": "Maitri", "Maitri Medicinals": "Maitri",
    "Modern Flower": "Modern Flower", "Modern Flower Ground": "Modern Flower",
    "mood": "mood", "mood by Vytal": "mood", "Mood by Vytal": "mood",
    "Ozone": "Ozone", "Ozone Reserve": "Ozone", "Penn Health": "PHG",
    "Penn Health Group": "PHG", "PHG": "PHG", "PhG": "PHG", "Prime": "Prime",
    "Prime Wellness": "Prime", "R.O.": "R.O.", "R.O. Ground": "R.O.",
    "R.O. Shake": "R.O.", "RYTHM": "Rythm", "Rythm": "Rythm",
    "SeCHe": "Seche", "Seche": "Seche", "Select": "Select", "Select Briq": "Select",
    "Select X": "Select", "Solventless by Vytal": "Vytal Solventless",
    "Vytal Solventless": "Vytal Solventless", "Strane": "Strane",
    "Strane Reserve": "Strane", "Strane Stash": "Strane",
    "Sunshine": "Sunshine", "Sunshine Cannabis": "Sunshine",
    "Supply/Cresco": "Supply", "Vytal": "Vytal", "Vytal Options": "Vytal",
    "*Pitt Promo*Belushi's Farm": "Belushi's Farm",
    "Black Budda": "Black Buddha",
    "Calypso Bountiful": "Calypso",
    "Jim's Stash": "Belushi's Farm",
    "Redemption Shake": "Redemption",
    "The Essence": "(the) Essence",
    "Verano Essence": "(the) Essence",
    "Moxie - PA": "Moxie",
    "RYTHM": "Rythm",
    "Flower by Edie Parker": "Edie Parker",
    "Black Buddha Cannabis": "Black Buddha",
}

MASTER_CATEGORY_MAP = {
    # Concentrates
    'Concentrate': 'Concentrates', 'Concentrates': 'Concentrates', 'concentrates': 'Concentrates',
    'extract': 'Concentrates', # <-- ADD THIS

    # Edibles
    'Edible': 'Edibles', 'Edibles': 'Edibles', 'edibles': 'Edibles',
    'edible': 'Edibles',     # <-- ADD THIS

    # Flower
    'Flower': 'Flower', 'flower': 'Flower',
    
    # Orals
    'ORALS': 'Orals', 'Oral': 'Orals', 'orals': 'Orals',
    
    # Tinctures
    'TINCTURES': 'Tinctures', 'Tincture': 'Tinctures', 'tinctures': 'Tinctures',
    'tincture': 'Tinctures',  # <-- ADD THIS

    # Topicals
    'Topicals': 'Topicals', 'TOPICALS': 'Topicals', 'topicals': 'Topicals',
    'topical': 'Topicals',    # <-- ADD THIS

    # Vaporizers
    'Vaporizers': 'Vaporizers', 'vaporizers': 'Vaporizers', 'vapes': 'Vaporizers',
    'vape': 'Vaporizers'      # <-- ADD THIS
    
    # We will ignore 'gear' as it's not a medication
}

MASTER_SUBCATEGORY_MAP = {
    # Flower Subtypes
    'WHOLE_FLOWER': 'Flower', 'Flower': 'Flower', 'Premium Flower': 'Flower',
    'premium': 'Flower', 'Bud': 'Flower', 'smalls': 'Small Buds',
    'SMALL_BUDS': 'Small Buds', 'Popcorn': 'Small Buds', 'Mini Buds': 'Small Buds',
    'SHAKE_TRIM': 'Ground/Shake', 'shake': 'Ground/Shake', 'Ground Flower': 'Ground/Shake',
    'PRE_GROUND': 'Ground/Shake',
    # Vaporizer Subtypes
    'CARTRIDGES': 'Cartridge', 'cartridge': 'Cartridge',
    'cured-resin-cartridge': 'Cartridge', 'live-resin-cartridge': 'Cartridge',
    'disposable_pen': 'Cartridge', 'disposables': 'Cartridge',
    # Concentrate Subtypes
    'LIVE_RESIN': 'Live Resin', 'Live Resin': 'Live Resin', 'live_resin': 'Live Resin',
    'ROSIN': 'Rosin', 'Rosin': 'Rosin', 'rosin': 'Rosin', 'RSO': 'RSO', 'rso': 'RSO',
    'SHATTER': 'Shatter', 'shatter': 'Shatter', 'SUGAR': 'Sugar', 'sugar': 'Sugar',
    'BADDER': 'Badder', 'badder': 'Badder', 'BUDDER': 'Budder', 'budder': 'Budder',
    'CRUMBLE': 'Crumble', 'crumble': 'Crumble', 'WAX': 'Wax', 'wax': 'Wax',
    'KIEF': 'Kief', 'kief': 'Kief',
}

# scrapers/scraper_utils.py

# ... (keep all the other maps above this) ...

MASTER_COMPOUND_MAP = {
    # --- Cannabinoids (from all scrapers) ---
    '"TAC\\" - Total Active Cannabinoids"': "TAC",
    'CBD': 'CBD',
    'CBDA': 'CBDa', 'CBDA (Cannabidiolic acid)': 'CBDa',
    'CBG': 'CBG', 'CBG (Cannabigerol)': 'CBG',
    'CBGA': 'CBGa', 'CBGA (Cannabigerolic acid)': 'CBGa',
    'CBN': 'CBN',
    'd8-THC': 'Delta-8 THC',
    'THC': 'THC', 'THC-D9 (Delta 9–tetrahydrocannabinol)': 'THC', 'Total_THC': 'THC',
    'THCA': 'THCa', 'THCA (Δ9-tetrahydrocannabinolic acid)': 'THCa',
    'THCV': 'THCv', 'thcv': 'THCv',

    # --- Terpenes (Comprehensive list from all scraper debugging) ---
    
    # Total Terps (Sweed/Cresco)
    'Total_Terpenes': 'Total_Terps', 'total_terps': 'Total_Terps',

    # a-Terpinene
    'a_terpinene': 'alpha-Terpinene', 'alpha-Terpinene': 'alpha-Terpinene', 'A Pinene': 'alpha-Terpinene',

    # alpha-Bisabolol
    'alpha-Bisabolol': 'alpha-Bisabolol', 'Bisabolol': 'alpha-Bisabolol', 'bisabolol': 'alpha-Bisabolol',
    'BISABOLOL': 'alpha-Bisabolol',
    
    # beta-Caryophyllene
    'b_caryophyllene': 'beta-Caryophyllene', 'B-Caryophyllene': 'beta-Caryophyllene',
    'Beta Caryophyllene': 'beta-Caryophyllene', 'BetaCaryophyllene': 'beta-Caryophyllene',
    'Caryophyllene': 'beta-Caryophyllene', 'caryophyllene': 'beta-Caryophyllene',
    'CARYOPHYLLENE': 'beta-Caryophyllene', 'B_CARYOPHYLLENE': 'beta-Caryophyllene',

    # beta-Myrcene
    'b_myrcene': 'beta-Myrcene', 'B-Myrcene': 'beta-Myrcene',
    'Beta Myrcene': 'beta-Myrcene', 'BetaMyrcene': 'beta-Myrcene',
    'Myrcene': 'beta-Myrcene', 'myrcene': 'beta-Myrcene',
    'MYRCENE': 'beta-Myrcene', 'B_MYRCENE': 'beta-Myrcene',
    
    # beta-Pinene
    'b_pinene': 'beta-Pinene', 'B-Pinene': 'beta-Pinene',
    'Beta Pinene': 'beta-Pinene', 'BetaPinene': 'beta-Pinene',
    'B_PINENE': 'beta-Pinene',

    # alpha-Pinene
    'a-Pinene': 'alpha-Pinene', 'alpha-Pinene': 'alpha-Pinene', 'A Pinene': 'alpha-Pinene',
    'Alpha Pinene': 'alpha-Pinene',

    # Camphene
    'Camphene': 'Camphene', 'camphene': 'Camphene', 'CAMPHENE': 'Camphene',
    
    # Carene
    'Carene': 'Carene', 'carene': 'Carene',
    
    # Caryophyllene Oxide
    'caryophyllene_oxide': 'Caryophyllene Oxide', 'CaryophylleneOxide': 'Caryophyllene Oxide',
    'Carophyllene Oxide': 'Caryophyllene Oxide', 'CAROPHYLLENE_OXIDE': 'Caryophyllene Oxide',

    # Eucalyptol
    'Eucalyptol': 'Eucalyptol', 'eucalyptol': 'Eucalyptol',
    
    # Farnesene
    'Farnesene': 'Farnesene', 'farnesene': 'Farnesene',
    
    # Geraniol
    'Geraniol': 'Geraniol', 'geraniol': 'Geraniol',
    
    # Guaiol
    'Guaiol': 'Guaiol', 'guaiol': 'Guaiol',
    
    # Humulene
    'Humulene': 'Humulene', 'humulene': 'Humulene',
    'HUMULENE': 'Humulene',
    
    # Limonene
    'Limonene': 'Limonene', 'limonene': 'Limonene',
    'LIMONENE': 'Limonene',
    
    # Linalool
    'Linalool': 'Linalool', 'linalool': 'Linalool',
    'LINALOOL': 'Linalool',
    
    # Ocimene
    'Ocimene': 'Ocimene', 'ocimene': 'Ocimene',
    
    # p-Cymene
    'p_cymene': 'p-Cymene', 'p-Cymene': 'p-Cymene',
    
    # Terpineol
    'Terpineol': 'Terpineol',
    
    # Terpinolene
    'Terpinolene': 'Terpinolene', 'terpinolene': 'Terpinolene',
    
    # trans-Nerolidol
    'trans_neridal': 'trans-Nerolidol', # Cresco typo
    'trans_nerolidol': 'trans-Nerolidol', 'trans-Nerolidol': 'trans-Nerolidol',
    'Nerolidol': 'trans-Nerolidol',
    
    # gamma-Terpinene
    'y_terpinene': 'gamma-Terpinene', 'y-Terpinene': 'gamma-Terpinene',
    
    # Pinene (Total)
    'Pinene': 'Pinene (Total)', 'pinene': 'Pinene (Total)',
    'PINENE': 'Pinene (Total)'
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
