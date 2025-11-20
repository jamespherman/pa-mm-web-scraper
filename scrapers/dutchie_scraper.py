# scrapers/dutchie_scraper.py
# -----------------------------------------------------------------------------
# This scraper handles the Dutchie e-commerce platform.
#
# Dutchie is complex because it uses "GraphQL". Instead of asking for a specific
# "page" like a normal website, we have to send a query describing exactly what
# data we want (e.g., "give me the name, price, and terpenes for product X").
#
# This scraper works in two steps:
# 1. "Slugs": Get a list of all product IDs and basic info (Name, Price).
# 2. "Details": For each unique product, send a second query to get the
#    detailed scientific data (Terpenes, Cannabinoids).
#
# We group products by "Batch Signature" to avoid asking for the same details
# twice (e.g., if a store has "Blue Dream 3.5g" and "Blue Dream 7g" from the
# same batch, the scientific data is likely the same).
# -----------------------------------------------------------------------------

import requests # For sending internet requests.
import pandas as pd # For data tables.
import numpy as np # For math/NaN.
import json # For handling JSON data (used heavily in GraphQL).
import re # For text pattern matching.
from .scraper_utils import (
    convert_to_grams, save_raw_json, BRAND_MAP, MASTER_CATEGORY_MAP,
    MASTER_SUBCATEGORY_MAP, MASTER_COMPOUND_MAP
)

# --- Constants ---

# The DUTCHIE_STORES dictionary contains the configuration for each store.
# Because Dutchie hosts many different dispensaries, each one might have a
# slightly different URL or "Store ID".
# We also need specific "headers" (like the x-dutchie-session) to be allowed in.
DUTCHIE_STORES = {
    # --- CURALEAF ---
    "Curaleaf (Gettysburg)": {
        "api_url": "https://curaleaf.com/api-2/graphql",
        "store_id": "6074c37fcee012009f173ff2",
        "headers": {
            "accept": "*/*", "apollographql-client-name": "Marketplace (production)", "content-type": "application/json",
            'cookie': 'confirmed21OrOlder=1',
            "referer": "https://curaleaf.com/stores/curaleaf-pa-gettysburg/products/flower",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15",
            "x-dutchie-session": "eyJpZCI6IjE5NTA1MGVmLTQ2MzMtNGRhYS05YjA5LTc4MzQ1ZDU0MTlhMSIsImV4cGlyZXMiOjE3NjM0ODcxMDExMTd9"
        }
    },
    "Curaleaf (Brookville)": {
        "api_url": "https://curaleaf.com/api-2/graphql",
        "store_id": "6074c3a031e11800c36bd129",
        "headers": {
            "accept": "*/*", "apollographql-client-name": "Marketplace (production)", "content-type": "application/json",
            'cookie': 'confirmed21OrOlder=1',
            "referer": "https://curaleaf.com/stores/curaleaf-pa-brookville/products/flower",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15",
            "x-dutchie-session": "eyJpZCI6IjE5NTA1MGVmLTQ2MzMtNGRhYS05YjA5LTc4MzQ1ZDU0MTlhMSIsImV4cGlyZXMiOjE3NjM0ODcxMDExMTd9"
        }
    },
    "Curaleaf (Morton)": {
        "api_url": "https://curaleaf.com/api-2/graphql",
        "store_id": "6074c3c505f7ee00caefc167",
        "headers": {
            "accept": "*/*", "apollographql-client-name": "Marketplace (production)", "content-type": "application/json",
            'cookie': 'confirmed21OrOlder=1',
            "referer": "https://curaleaf.com/stores/curaleaf-pa-morton/products/flower",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15",
            "x-dutchie-session": "eyJpZCI6IjE5NTA1MGVmLTQ2MzMtNGRhYS05YjA5LTc4MzQ1ZDU0MTlhMSIsImV4cGlyZXMiOjE3NjM0ODcxMDExMTd9"
        }
    },
    "Curaleaf (Altoona)": {
        "api_url": "https://curaleaf.com/api-2/graphql",
        "store_id": "6074c3e954b6a800d8c7ba0f",
        "headers": {
            "accept": "*/*", "apollographql-client-name": "Marketplace (production)", "content-type": "application/json",
            'cookie': 'confirmed21OrOlder=1',
            "referer": "https://curaleaf.com/stores/curaleaf-pa-altoona/products/flower",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15",
            "x-dutchie-session": "eyJpZCI6IjE5NTA1MGVmLTQ2MzMtNGRhYS05YjA5LTc4MzQ1ZDU0MTlhMSIsImV4cGlyZXMiOjE3NjM0ODcxMDExMTd9"
        }
    },
    "Curaleaf (Lebanon)": {
        "api_url": "https://curaleaf.com/api-2/graphql",
        "store_id": "6074c411e5801600aea48226",
        "headers": {
            "accept": "*/*", "apollographql-client-name": "Marketplace (production)", "content-type": "application/json",
            'cookie': 'confirmed21OrOlder=1',
            "referer": "https://curaleaf.com/stores/curaleaf-pa-lebanon/products/flower",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15",
            "x-dutchie-session": "eyJpZCI6IjE5NTA1MGVmLTQ2MzMtNGRhYS05YjA5LTc4MzQ1ZDU0MTlhMSIsImV4cGlyZXMiOjE3NjM0ODcxMDExMTd9"
        }
    },
    "Curaleaf (King of Prussia)": {
        "api_url": "https://curaleaf.com/api-2/graphql",
        "store_id": "6074c4351f698400aec35540",
        "headers": {
            "accept": "*/*", "apollographql-client-name": "Marketplace (production)", "content-type": "application/json",
            'cookie': 'confirmed21OrOlder=1',
            "referer": "https://curaleaf.com/stores/curaleaf-pa-king-of-prussia/products/flower",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15",
            "x-dutchie-session": "eyJpZCI6IjE5NTA1MGVmLTQ2MzMtNGRhYS05YjA5LTc4MzQ1ZDU0MTlhMSIsImV4cGlyZXMiOjE3NjM0ODcxMDExMTd9"
        }
    },
    "Curaleaf (Bradford)": {
        "api_url": "https://curaleaf.com/api-2/graphql",
        "store_id": "6074c45373ad3500ad4ebdf3",
        "headers": {
            "accept": "*/*", "apollographql-client-name": "Marketplace (production)", "content-type": "application/json",
            'cookie': 'confirmed21OrOlder=1',
            "referer": "https://curaleaf.com/stores/curaleaf-pa-bradford/products/flower",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15",
            "x-dutchie-session": "eyJpZCI6IjE5NTA1MGVmLTQ2MzMtNGRhYS05YjA5LTc4MzQ1ZDU0MTlhMSIsImV4cGlyZXMiOjE3NjM0ODcxMDExMTd9"
        }
    },
    "Curaleaf (Philadelphia)": {
        "api_url": "https://curaleaf.com/api-2/graphql",
        "store_id": "6074c46d7aab5200c9c1ce26",
        "headers": {
            "accept": "*/*", "apollographql-client-name": "Marketplace (production)", "content-type": "application/json",
            'cookie': 'confirmed21OrOlder=1',
            "referer": "https://curaleaf.com/stores/curaleaf-pa-philadelphia/products/flower",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15",
            "x-dutchie-session": "eyJpZCI6IjE5NTA1MGVmLTQ2MzMtNGRhYS05YjA5LTc4MzQ1ZDU0MTlhMSIsImV4cGlyZXMiOjE3NjM0ODcxMDExMTd9"
        }
    },
    "Curaleaf (DuBois)": {
        "api_url": "https://curaleaf.com/api-2/graphql",
        "store_id": "6074c493f7d2f400c2e1e282",
        "headers": {
            "accept": "*/*", "apollographql-client-name": "Marketplace (production)", "content-type": "application/json",
            'cookie': 'confirmed21OrOlder=1',
            "referer": "https://curaleaf.com/stores/curaleaf-pa-dubois/products/flower",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15",
            "x-dutchie-session": "eyJpZCI6IjE5NTA1MGVmLTQ2MzMtNGRhYS05YjA5LTc4MzQ1ZDU0MTlhMSIsImV4cGlyZXMiOjE3NjM0ODcxMDExMTd9"
        }
    },
    "Curaleaf (Harrisburg)": {
        "api_url": "https://curaleaf.com/api-2/graphql",
        "store_id": "6074c4b9b29d5d00ada48f5e",
        "headers": {
            "accept": "*/*", "apollographql-client-name": "Marketplace (production)", "content-type": "application/json",
            'cookie': 'confirmed21OrOlder=1',
            "referer": "https://curaleaf.com/stores/curaleaf-pa-harrisburg/products/flower",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15",
            "x-dutchie-session": "eyJpZCI6IjE5NTA1MGVmLTQ2MzMtNGRhYS05YjA5LTc4MzQ1ZDU0MTlhMSIsImV4cGlyZXMiOjE3NjM0ODcxMDExMTd9"
        }
    },
    "Curaleaf (City Ave, Philadelphia)": {
        "api_url": "https://curaleaf.com/api-2/graphql",
        "store_id": "6074c4e35e456200ae8fd73c",
        "headers": {
            "accept": "*/*", "apollographql-client-name": "Marketplace (production)", "content-type": "application/json",
            'cookie': 'confirmed21OrOlder=1',
            "referer": "https://curaleaf.com/stores/curaleaf-pa-city-ave-philadelphia/products/flower",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15",
            "x-dutchie-session": "eyJpZCI6IjE5NTA1MGVmLTQ2MzMtNGRhYS05YjA5LTc4MzQ1ZDU0MTlhMSIsImV4cGlyZXMiOjE3NjM0ODcxMDExMTd9"
        }
    },
    "Curaleaf (Horsham)": {
        "api_url": "https://curaleaf.com/api-2/graphql",
        "store_id": "6074c502db8237009eb9aac0",
        "headers": {
            "accept": "*/*", "apollographql-client-name": "Marketplace (production)", "content-type": "application/json",
            'cookie': 'confirmed21OrOlder=1',
            "referer": "https://curaleaf.com/stores/curaleaf-pa-horsham/products/flower",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15",
            "x-dutchie-session": "eyJpZCI6IjE5NTA1MGVmLTQ2MzMtNGRhYS05YjA5LTc4MzQ1ZDU0MTlhMSIsImV4cGlyZXMiOjE3NjM0ODcxMDExMTd9"
        }
    },
    "Curaleaf (State College)": {
        "api_url": "https://curaleaf.com/api-2/graphql",
        "store_id": "61fa091869e083009ea12aef",
        "headers": {
            "accept": "*/*", "apollographql-client-name": "Marketplace (production)", "content-type": "application/json",
            'cookie': 'confirmed21OrOlder=1',
            "referer": "https://curaleaf.com/stores/curaleaf-pa-state-college/products/flower",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15",
            "x-dutchie-session": "eyJpZCI6IjE5NTA1MGVmLTQ2MzMtNGRhYS05YjA5LTc4MzQ1ZDU0MTlhMSIsImV4cGlyZXMiOjE3NjM0ODcxMDExMTd9"
        }
    },
    "Curaleaf (Erie)": {
        "api_url": "https://curaleaf.com/api-2/graphql",
        "store_id": "61fa09385eb1f200a6329407",
        "headers": {
            "accept": "*/*", "apollographql-client-name": "Marketplace (production)", "content-type": "application/json",
            'cookie': 'confirmed21OrOlder=1',
            "referer": "https://curaleaf.com/stores/curaleaf-pa-erie/products/flower",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15",
            "x-dutchie-session": "eyJpZCI6IjE5NTA1MGVmLTQ2MzMtNGRhYS05YjA5LTc4MzQ1ZDU0MTlhMSIsImV4cGlyZXMiOjE3NjM0ODcxMDExMTd9"
        }
    },
    "Curaleaf (Greensburg)": {
        "api_url": "https://curaleaf.com/api-2/graphql",
        "store_id": "61fa0958efd7100091033f5d",
        "headers": {
            "accept": "*/*", "apollographql-client-name": "Marketplace (production)", "content-type": "application/json",
            'cookie': 'confirmed21OrOlder=1',
            "referer": "https://curaleaf.com/stores/curaleaf-pa-greensburg/products/vaporizers?sortby=relevance",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0",
            "x-dutchie-session": "eyJpZCI6IjE5NTA1MGVmLTQ2MzMtNGRhYS05YjA5LTc4MzQ1ZDU0MTlhMSIsImV4cGlyZXMiOjE3NjM0ODcxMDExMTd9"
        }
    },
    "Curaleaf (Wayne)": {
        "api_url": "https://curaleaf.com/api-2/graphql",
        "store_id": "61fa08f96cd65800891fad7c",
        "headers": {
            "accept": "*/*", "apollographql-client-name": "Marketplace (production)", "content-type": "application/json",
            'cookie': 'confirmed21OrOlder=1',
            "referer": "https://curaleaf.com/stores/curaleaf-pa-wayne/products/flower",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15",
            "x-dutchie-session": "eyJpZCI6IjE5NTA1MGVmLTQ2MzMtNGRhYS05YjA5LTc4MzQ1ZDU0MTlhMSIsImV4cGlyZXMiOjE3NjM0ODcxMDExMTd9"
        }
    },
    # --- ETHOS ---
    "Ethos (Harmarville)": {
        "api_url": "https://nephilly.ethoscannabis.com/api-4/graphql",
        "store_id": "621900cebbc5580e15476deb",
        "headers": {
            'accept': '*/*',
            'apollographql-client-name': 'Marketplace (production)',
            'content-type': 'application/json',
            'referer': 'https://nephilly.ethoscannabis.com/stores/ethos-harmarville',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0',
            'x-dutchie-session': 'eyJpZCI6IjE5Y2MzYWIyLTc4NGEtNDVhZC05ZDVlLTVmOTM3YzdiYzdmMyIsImV4cGlyZXMiOjE3NjM3Mzk5OTMyMTF9'
        }
    },
    "Ethos (Philadelphia)": {
        "api_url": "https://nephilly.ethoscannabis.com/api-4/graphql",
        "store_id": "607f5e79490cc600c0d588d1",
        "headers": {
            'accept': '*/*',
            'apollographql-client-name': 'Marketplace (production)',
            'content-type': 'application/json',
            'referer': 'https://nephilly.ethoscannabis.com/stores/ethos-philadelphia',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0',
            'x-dutchie-session': 'eyJpZCI6IjE5Y2MzYWIyLTc4NGEtNDVhZC05ZDVlLTVmOTM3YzdiYzdmMyIsImV4cGlyZXMiOjE3NjM3Mzk5OTMyMTF9'
        }
    },
    "Ethos (Montgomeryville)": {
        "api_url": "https://nephilly.ethoscannabis.com/api-4/graphql",
        "store_id": "5f2de49198211000abef8b99",
        "headers": {
            'accept': '*/*',
            'apollographql-client-name': 'Marketplace (production)',
            'content-type': 'application/json',
            'referer': 'https://nephilly.ethoscannabis.com/stores/ethos-montgomeryville',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0',
            'x-dutchie-session': 'eyJpZCI6IjE5Y2MzYWIyLTc4NGEtNDVhZC05ZDVlLTVmOTM3YzdiYzdmMyIsImV4cGlyZXMiOjE3NjM3Mzk5OTMyMTF9'
        }
    },
    "Ethos (Allentown)": {
        "api_url": "https://nephilly.ethoscannabis.com/api-4/graphql",
        "store_id": "4bZmK4MfjoypZ8MdN",
        "headers": {
            'accept': '*/*',
            'apollographql-client-name': 'Marketplace (production)',
            'content-type': 'application/json',
            'referer': 'https://nephilly.ethoscannabis.com/stores/ethos-allentown',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0',
            'x-dutchie-session': 'eyJpZCI6IjE5Y2MzYWIyLTc4NGEtNDVhZC05ZDVlLTVmOTM3YzdiYzdmMyIsImV4cGlyZXMiOjE3NjM3Mzk5OTMyMTF9'
        }
    },
    "Ethos (Hazleton)": {
        "api_url": "https://nephilly.ethoscannabis.com/api-4/graphql",
        "store_id": "5fad9a6840352500ba68def0",
        "headers": {
            'accept': '*/*',
            'apollographql-client-name': 'Marketplace (production)',
            'content-type': 'application/json',
            'referer': 'https://nephilly.ethoscannabis.com/stores/ethos-hazleton',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0',
            'x-dutchie-session': 'eyJpZCI6IjE5Y2MzYWIyLTc4NGEtNDVhZC05ZDVlLTVmOTM3YzdiYzdmMyIsImV4cGlyZXMiOjE3NjM3Mzk5OTMyMTF9'
        }
    },
    "Ethos (Wilkes-Barre)": {
        "api_url": "https://nephilly.ethoscannabis.com/api-4/graphql",
        "store_id": "5f4ef2d0b28822768a8a574c",
        "headers": {
            'accept': '*/*',
            'apollographql-client-name': 'Marketplace (production)',
            'content-type': 'application/json',
            'referer': 'https://nephilly.ethoscannabis.com/stores/ethos-wilkes-barre',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0',
            'x-dutchie-session': 'eyJpZCI6IjE5Y2MzYWIyLTc4NGEtNDVhZC05ZDVlLTVmOTM3YzdiYzdmMyIsImV4cGlyZXMiOjE3NjM3Mzk5OTMyMTF9'
        }
    },
    "Ethos (Pittsburgh West)": {
        "api_url": "https://nephilly.ethoscannabis.com/api-4/graphql",
        "store_id": "5fa0829005bb2400cfc4b694",
        "headers": {
            'accept': '*/*',
            'apollographql-client-name': 'Marketplace (production)',
            'content-type': 'application/json',
            'referer': 'https://nephilly.ethoscannabis.com/stores/ethos-pittsburgh-west',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/1L2.0.0.0',
            'x-dutchie-session': 'eyJpZCI6IjE5Y2MzYWIyLTc4NGEtNDVhZC05ZDVlLTVmOTM3YzdiYzdmMyIsImV4cGlyZXMiOjE3NjM3Mzk5OTMyMTF9'
        }
    },
    "Ethos (Pleasant Hills)": {
        "api_url": "https://nephilly.ethoscannabis.com/api-4/graphql",
        "store_id": "607dc27bfde18500b5e8dd52",
        "headers": {
            'accept': '*/*',
            'apollographql-client-name': 'Marketplace (production)',
            'content-type': 'application/json',
            'referer': 'https://nephilly.ethoscannabis.com/stores/ethos-pleasant-hills',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0',
            'x-dutchie-session': 'eyJpZCI6IjE5Y2MzYWIyLTc4NGEtNDVhZC05ZDVlLTVmOTM3YzdiYzdmMyIsImV4cGlyZXMiOjE3NjM3Mzk5OTMyMTF9'
        }
    },

    # --- ASCEND ---
    "Ascend (Cranberry)": {
        "api_url": "https://letsascend.com/api-4/graphql",
        "store_id": "66fef50576b5d1b3703a1890",
        "headers": {
            "accept": "*/*", "apollographql-client-name": "Marketplace (production)", "content-type": "application/json",
            "referer": "https://letsascend.com/stores/cranberry-pennsylvania",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15",
            "x-dutchie-session": "eyJpZCI6IjNhMTFmZGZhLTU5MGQtNDk5ZC1hYzE4LTRjNjhlZjRjNjZkNiIsImV4cGlyZXMiOjE3NjI0ODA3NzY0ODF9"
        }
    },
    "Ascend (Monaca)": {
        "api_url": "https://letsascend.com/api-4/graphql",
        "store_id": "66fef58038ff55ae0d700b55",
        "headers": {
            "accept": "*/*", "apollographql-client-name": "Marketplace (production)", "content-type": "application/json",
            "referer": "https://letsascend.com/stores/monaca-pennsylvania",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15",
            "x-dutchie-session": "eyJpZCI6IjNhMTFmZGZhLTU5MGQtNDk5ZC1hYzE4LTRjNjhlZjRjNjZkNiIsImV4cGlyZXMiOjE3NjI0ODA3NzY0ODF9"
        }
    },
    "Ascend (Scranton)": {
        "api_url": "https://letsascend.com/api-4/graphql",
        "store_id": "66fef532110068aee1c6b99d",
        "headers": {
            "accept": "*/*", "apollographql-client-name": "Marketplace (production)", "content-type": "application/json",
            "referer": "https://letsascend.com/stores/wayne-pennsylvania",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15",
            "x-dutchie-session": "eyJpZCI6IjNhMTFmZGZhLTU5MGQtNDk5ZC1hYzE4LTRjNjhlZjRjNjZkNiIsImV4cGlyZXMiOjE3NjI0ODA3NzY0ODF9"
        }
    },
    "Ascend (Wayne)": {
        "api_url": "https://letsascend.com/api-4/graphql",
        "store_id": "66fef5589eb852714bc99c0c",
        "headers": {
            "accept": "*/*", "apollographql-client-name": "Marketplace (production)", "content-type": "application/json",
            "referer": "https://letsascend.com/stores/wayne-pennsylvania",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15",
            "x-dutchie-session": "eyJpZCI6IjNhMTFmZGZhLTU5MGQtNDk5ZC1hYzE4LTRjNjhlZjRjNjZkNiIsImV4cGlyZXMiOjE3NjI0ODA3NzY0ODF9"
        }
    },
    "Ascend (Whitehall)": {
        "api_url": "https://letsascend.com/api-4/graphql",
        "store_id": "66c371484a1610802761aa4c",
        "headers": {
            "accept": "*/*", "apollographql-client-name": "Marketplace (production)", "content-type": "application/json",
            "referer": "https://letsascend.com/stores/wayne-pennsylvania",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15",
            "x-dutchie-session": "eyJpZCI6IjNhMTFmZGZhLTU5MGQtNDk5ZC1hYzE4LTRjNjhlZjRjNjZkNiIsImV4cGlyZXMiOjE3NjI0ODA3NzY0ODF9"
        }
    },
}

def _normalize_name_for_grouping(name):
    """
    Creates a simplified 'fingerprint' of a product name for fuzzy matching.

    Why? Some stores list "Blue Dream Flower" and "Blue Dream Premium Flower".
    We want to treat these as the same "Batch" to avoid double-fetching data.

    What it does:
    1. Converts to lowercase.
    2. Removes punctuation.
    3. Removes "noise words" like 'flower', 'premium', 'hybrid', '1g', '3.5g'.
    """
    if not name: return ""
    
    # 1. Lowercase and remove non-alphanumeric characters (keep only a-z and 0-9)
    clean = re.sub(r'[^a-z0-9]', '', name.lower())
    
    # 2. Remove common 'menu noise' words that don't change the chemical profile
    noise_words = [
        'flower', 'premium', 'whole', 'smalls', 'small', 'buds', 'bud',
        'grind', 'ground', 'shake', 'trim', 'popcorn', 'fine',
        'hybrid', 'indica', 'sativa', 'thc', 'cbd',
        'cartridge', 'vape', 'cart', 'disposable', 'pen', 'pod',
        'live', 'resin', 'rosin', 'sauce', 'badder', 'budder', 'sugar', 'crumble',
        'syringe', 'capsules', 'rso', 'pack', 'briq', 'elite',
        'g', 'mg', 'oz', 'gram', '1g', '35g', '7g', '14g', '28g', '05g', '2g', '1000mg', '100mg', '10', 'ea'
    ]
    
    for word in noise_words:
        clean = clean.replace(word, '')
        
    return clean
    
def get_all_product_slugs(store_name, store_config):
    """
    Step 1: Fetch basic product info (Slugs) for a store.

    This asks the API for a list of ALL products, but only asks for basic fields
    (Name, Brand, THC, Price, Weight). It does NOT ask for terpenes yet.

    Args:
        store_name (str): Human-readable name of the store.
        store_config (dict): Configuration dictionary (URL, ID, headers).

    Returns:
        list: A list of simplified product dictionaries.
    """
    all_products = []
    print(f"Step 1: Fetching product slugs for {store_name}...")
    
    api_url, store_id, headers = store_config['api_url'], store_config['store_id'], store_config['headers']
    page = 0
    
    while True:
        # The GraphQL Query Variables
        variables = {
            "includeEnterpriseSpecials": False,
            "productsFilter": {
                "dispensaryId": store_id, "pricingType": "med", "strainTypes": [], "subcategories": [],
                "Status": "Active", "types": [], "useCache": False, "isDefaultSort": False,
                "sortBy": "relevance", "sortDirection": 1, "bypassOnlineThresholds": False,
                "isKioskMenu": False, "removeProductsBelowOptionThresholds": True
            },
            "page": page, "perPage": 100
        }
        # The "Query Hash" identifies which query we want to run on the server.
        extensions = {"persistedQuery": {"version": 1, "sha256Hash": "ee29c060826dc41c527e470e9ae502c9b2c169720faa0a9f5d25e1b9a530a4a0"}}

        # Combine into parameters for the request
        params = {'operationName': 'FilteredProducts', 'variables': json.dumps(variables), 'extensions': json.dumps(extensions)}

        try:
            response = requests.get(api_url, headers=headers, params=params)
            response.raise_for_status()
            json_response = response.json()
            
            # Save raw list for debugging
            filename_parts = ['dutchie', store_name, 'products', f'p{page}']
            save_raw_json(json_response, filename_parts)
            
            if 'errors' in json_response:
                print(f"GraphQL Error in product slugs for {store_name}: {json_response['errors']}")
                break
                
            # Navigate deep into the JSON to find the list of products
            products = json_response.get('data', {}).get('filteredProducts', {}).get('products', [])
            if not products: break # If no products, we are done.

            for product in products:
                # Safely extract data, handling cases where values might be None (null)
                thc_data = product.get('THCContent') or {}
                thc_content = thc_data.get('range', [0])

                cbd_data = product.get('CBDContent') or {}
                cbd_content = cbd_data.get('range', [0])
                
                # Get Price (Medical preferred, fallback to Rec)
                prices = product.get('medicalPrices') or product.get('recPrices') or []
                price = min(prices) if prices else 0
                
                # Get Weight (usually the first option)
                options = product.get('Options', [])
                weight = options[0] if options else "N/A"

                all_products.append({
                    "cName": product['cName'], # The "canonical name" used for the next query
                    "DispensaryID": store_id,
                    "StoreName": store_name,
                    "StoreConfig": store_config,
                    # Metadata for Grouping & Final Data
                    "Name": product.get('Name'),
                    "Brand": product.get('brandName'),
                    "THC": thc_content[0] if thc_content else 0,
                    "CBD": cbd_content[0] if cbd_content else 0,
                    "Price": price,
                    "Weight_Str": weight,
                    "Type": product.get('type'),
                    "Subtype": product.get('subcategory')
                })
            page += 1
        except requests.exceptions.RequestException as e:
            print(f"Error fetching product slugs for {store_name}: {e}")
            break
        except KeyError:
            print(f"Unexpected JSON structure for {store_name}.")
            break

    print(f"  ...found {len(all_products)} total products for {store_name}.")
    return all_products

def get_detailed_product_info(product_list):
    """
    Step 2: Group products and fetch detailed info (Terpenes).

    Instead of making 1000 API calls for 1000 products, we group them.
    If we see 5 products that look like "Cresco Bio Jesus" with the same THC/CBD,
    we assume they are from the same batch. We fetch the details for ONE of them,
    and apply those details (like Terpenes) to all 5.
    """
    all_product_data = []
    print("\nStep 2: Optimizing and fetching details...")

    # --- 1. Group products by "Batch Signature" ---
    product_groups = {}
    
    for p in product_list:
        # Create the Fuzzy Name Fingerprint
        norm_name = _normalize_name_for_grouping(p['Name'])
        
        # Create the Unique Batch Key: Brand + Weight + Potency + Name
        key = (
            p['Brand'],
            p['Weight_Str'],
            f"{p['THC']:.2f}",
            f"{p['CBD']:.2f}",
            norm_name
        )
        
        if key not in product_groups:
            product_groups[key] = []
        product_groups[key].append(p)
    
    total_products = len(product_list)
    unique_batches = len(product_groups)
    
    print(f"  ...Optimized: {total_products} listings condensed into {unique_batches} unique batches.")
    print(f"  ...Efficiency gain: {((total_products - unique_batches) / total_products) * 100:.1f}% reduction in calls.")

    # --- 2. Iterate through unique batches ---
    for i, (key, group_items) in enumerate(product_groups.items()):
        if (i + 1) % 50 == 0:
            print(f"  ...processing batch {i + 1}/{unique_batches}")

        # Use the first item as the "Representative" to fetch data
        representative = group_items[0]
        
        # Make the API call (Once per group)
        cName = representative['cName']
        store_config = representative['StoreConfig']
        
        variables = {
            "includeTerpenes": True, "includeCannabinoids": True, "includeEnterpriseSpecials": False,
            "productsFilter": {
                "cName": cName, "dispensaryId": representative['DispensaryID'],
                "removeProductsBelowOptionThresholds": False, "isKioskMenu": False,
                "bypassKioskThresholds": False, "bypassOnlineThresholds": True, "Status": "All"
            }
        }
        extensions = {"persistedQuery": {"version": 1, "sha256Hash": "47369a02fc8256aaf1ed70d0c958c88514acdf55c5810a5be8e0ee1a19617cda"}}
        params = {'operationName': 'IndividualFilteredProduct', 'variables': json.dumps(variables), 'extensions': json.dumps(extensions)}

        try:
            response = requests.get(store_config['api_url'], headers=store_config['headers'], params=params)
            response.raise_for_status()
            json_response = response.json()

            # Save the raw JSON data for this batch
            filename_parts = ['dutchie', representative['StoreName'], 'product_details', cName]
            save_raw_json(json_response, filename_parts)
            
            products_resp = json_response.get('data', {}).get('filteredProducts', {}).get('products', [])
            
            if products_resp:
                # Parse the rich data (Terpenes!) from the representative
                detail_data = parse_product_details(products_resp[0], representative['StoreName'])
                if not detail_data: detail_data = {}
            else:
                detail_data = {}

        except Exception as e:
            print(f"Error fetching details for {cName}: {e}")
            detail_data = {}

        # --- 3. Distribute data to ALL group members ---
        for item in group_items:
            # 1. Start with the basic data we already scraped (Price, Store, etc.)
            final_item = {
                'Name': item['Name'],
                'Brand': BRAND_MAP.get(item['Brand'], item['Brand']),
                'Store': item['StoreName'],
                'Type': MASTER_CATEGORY_MAP.get(item['Type'], item['Type']),
                'Subtype': MASTER_SUBCATEGORY_MAP.get(item['Subtype'], item['Subtype']),
                'Price': item['Price'],
                'Weight_Str': item['Weight_Str'],
                'Weight': convert_to_grams(item['Weight_Str']),
                'THC': item['THC'],
                'CBD': item['CBD']
            }
            
            # 2. Enrich with the fetched details (Terpenes!)
            # We merge the 'detail_data' into 'final_item'.
            for k, v in detail_data.items():
                if k not in final_item: # Only add missing keys (like Terpenes)
                    final_item[k] = v

            all_product_data.append(final_item)

    print(f"  ...successfully processed {len(all_product_data)} products.")
    return all_product_data

def parse_product_details(product, store_name):
    """
    Parses the complex, nested JSON of a single product into a flat dictionary.

    This is where we extract the specific chemical values (Terpenes, Cannabinoids).
    """
    
    # Standardize category
    category_name = product.get('type')
    standardized_category = MASTER_CATEGORY_MAP.get(category_name)
    if not standardized_category:
        return None

    # Standardize brand
    brand_name = (product.get('brandName') or 'N/A').strip()
    subcategory_name = product.get('subcategory')

    data = {
        'Name': product.get('Name', 'N/A'),
        'Brand': BRAND_MAP.get(brand_name, brand_name),
        'Type': standardized_category,
        'Subtype': MASTER_SUBCATEGORY_MAP.get(subcategory_name, subcategory_name),
        'Store': store_name
    }

    # Pricing and weight
    prices = product.get('medicalPrices', [])
    special_prices = product.get('medicalSpecialPrices', [])
    # Use special price if available, else regular price.
    data['Price'] = min(special_prices) if special_prices else (min(prices) if prices else np.nan)

    options = product.get('Options', [])
    weight_str = options[0] if options else None
    data['Weight'] = convert_to_grams(weight_str)
    data['Weight_Str'] = weight_str if weight_str else 'N/A'

    # Process compounds (cannabinoids and terpenes)
    compounds_dict = {}

    # --- Handle Terpenes ---
    terpenes_list = product.get('terpenes')
    if terpenes_list is not None:
        for item in terpenes_list:
            # Some terpenes are nested under 'libraryTerpene', some might be direct.
            name = item.get('libraryTerpene', {}).get('name')
            if name:
                standard_name = MASTER_COMPOUND_MAP.get(name)
                if standard_name:
                    compounds_dict[standard_name] = item.get('value')

    # --- Handle Cannabinoids ---
    cannabinoids_list = product.get('cannabinoidsV2')
    if cannabinoids_list is not None:
        for item in cannabinoids_list:
            name = item.get('cannabinoid', {}).get('name')
            if name:
                standard_name = MASTER_COMPOUND_MAP.get(name)
                if standard_name:
                    compounds_dict[standard_name] = item.get('value')

    data.update(compounds_dict)

    return data

def fetch_dutchie_data():
    """
    The main orchestration function for the Dutchie scraper.

    It loops through every store, gets the slugs, groups them, fetches details,
    and combines everything into a DataFrame.
    """
    all_store_slugs = []
    for store_name, store_config in DUTCHIE_STORES.items():
        all_store_slugs.extend(get_all_product_slugs(store_name, store_config))

    if not all_store_slugs:
        print("No product slugs found for any Dutchie store. Exiting Dutchie scraper.")
        return pd.DataFrame()
    
    # Get detailed info for all products
    product_details = get_detailed_product_info(all_store_slugs)

    if not product_details:
        print("No product data was fetched. Returning an empty DataFrame.")
        return pd.DataFrame()

    # Create DataFrame
    df = pd.DataFrame(product_details)

    # Calculate Dollars Per Gram
    df['dpg'] = df['Price'] / df['Weight']

    print("\nScraping complete for Dutchie stores. DataFrame created.")
    return df
