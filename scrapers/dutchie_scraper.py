# scrapers/dutchie_scraper.py
# This scraper is designed to fetch data from the Dutchie GraphQL API.
# Dutchie is a common platform for cannabis dispensaries, but each dispensary
# often has its own unique API endpoint and configuration. This scraper is

import requests
import pandas as pd
import numpy as np
import json
import pdb
from .scraper_utils import (
    convert_to_grams, BRAND_MAP, MASTER_CATEGORY_MAP,
    MASTER_SUBCATEGORY_MAP, MASTER_COMPOUND_MAP, save_raw_json
)

# --- Constants ---

# The DUTCHIE_STORES dictionary is the core configuration for this scraper.
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
    # NOTE: Using the comprehensive store list and cName-based referers.
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

#    # --- AYR ---
#    "Ayr (Gibsonia)": {
#        "api_url": "https://dutchie.com/api-2/graphql",
#        "store_id": "5ff8ee358174a300e11a15cb", # This ID was correct
#        "headers": {
#            "accept": "*/*", "apollographql-client-name": "Marketplace (production)", "content-type": "application/json",
#            "referer": "https://dutchie.com/embedded-menu/ayr-dispensary-gibsonia/carousels/a38fb001-0957-4287-9247-cff829021762?carouselId=a38fb001-0957-4287-9247-cff829021762&routeRoot=https%3A%2F%2Fayrdispensaries.com%2Fpennsylvania%2Fgibsonia%2Fshop%2F",
#            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15",
#            "x-dutchie-session": "eyJpZCI6Ijg3NDI3YWE0LWQyMjUtNDZkYi1hY2M2LTk2NjU3YmQyMTRjMCIsImV4cGlyZXMiOjE3NjM2MDgwMTE3NzB9"
#        }
#    },
#    "Ayr (Bryn Mawr)": {
#        "api_url": "https://dutchie.com/api-2/graphql",
#        "store_id": "607474136e6c2700e1d03328",
#        "headers": {
#            "accept": "*/*", "apollographql-client-name": "Marketplace (production)", "content-type": "application/json",
#            "referer": "https://ayrdispensaries.com/pennsylvania/bryn-mawr/shop/",
#            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15",
#            "x-dutchie-session": "eyJpZCI6Ijg3NDI3YWE0LWQyMjUtNDZkYi1hY2M2LTk2NjU3YmQyMTRjMCIsImV4cGlyZXMiOjE3NjM2MDgwMTE3NzB9"
#        }
#    },
#    "Ayr (New Castle)": {
#        "api_url": "https://dutchie.com/api-2/graphql",
#        "store_id": "6074744476081400e1215169",
#        "headers": {
#            "accept": "*/*", "apollographql-client-name": "Marketplace (production)", "content-type": "application/json",
#            "referer": "https://ayrdispensaries.com/pennsylvania/new-castle/shop/",
#            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15",
#            "x-dutchie-session": "eyJpZCI6Ijg3NDI3YWE0LWQyMjUtNDZkYi1hY2M2LTk2NjU3YmQyMTRjMCIsImV4cGlyZXMiOjE3NjM2MDgwMTE3NzB9"
#        }
#    },
#    "Ayr (Indiana)": {
#        "api_url": "https://dutchie.com/api-2/graphql",
#        "store_id": "622e0e01700689000c0f73f5",
#        "headers": {
#            "accept": "*/*", "apollographql-client-name": "Marketplace (production)", "content-type": "application/json",
#            "referer": "https://ayrdispensaries.com/pennsylvania/indiana/shop/",
#            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15",
#            "x-dutchie-session": "eyJpZCI6Ijg3NDI3YWE0LWQyMjUtNDZkYi1hY2M2LTk2NjU3YmQyMTRjMCIsImV4cGlyZXMiOjE3NjM2MDgwMTE3NzB9"
#        }
#    },
#    "Ayr (Plymouth Meeting)": {
#        "api_url": "https://dutchie.com/api-2/graphql",
#        "store_id": "6074742a76081400e1215163",
#        "headers": {
#            "accept": "*/*", "apollographql-client-name": "Marketplace (production)", "content-type": "application/json",
#            "referer": "https://ayrdispensaries.com/pennsylvania/plymouth-meeting/shop/",
#            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15",
#            "x-dutchie-session": "eyJpZCI6Ijg3NDI3YWE0LWQyMjUtNDZkYi1hY2M2LTk2NjU3YmQyMTRjMCIsImV4cGlyZXMiOjE3NjM2MDgwMTE3NzB9"
#        }
#    },
#    "Ayr (Bloomsburg)": {
#        "api_url": "https://dutchie.com/api-2/graphql",
#        "store_id": "6074745d76081400e121516f",
#        "headers": {
#            "accept": "*/*", "apollographql-client-name": "Marketplace (production)", "content-type": "application/json",
#            "referer": "https://ayrdispensaries.com/pennsylvania/bloomsburg/shop/",
#            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15",
#            "x-dutchie-session": "eyJpZCI6Ijg3NDI3YWE0LWQyMjUtNDZkYi1hY2M2LTk2NjU3YmQyMTRjMCIsImV4cGlyZXMiOjE3NjM2MDgwMTE3NzB9"
#        }
#    },
#    "Ayr (Selinsgrove)": {
#        "api_url": "https://dutchie.com/api-2/graphql",
#        "store_id": "61b369c00ddd67000d14b437",
#        "headers": {
#            "accept": "*/*", "apollographql-client-name": "Marketplace (production)", "content-type": "application/json",
#            "referer": "https://ayrdispensaries.com/pennsylvania/selinsgrove/shop/",
#            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15",
#            "x-dutchie-session": "eyJpZCI6Ijg3NDI3YWE0LWQyMjUtNDZkYi1hY2M2LTk2NjU3YmQyMTRjMCIsImV4cGlyZXMiOjE3NjM2MDgwMTE3NzB9"
#        }
#    },
#    "Ayr (State College)": {
#        "api_url": "https://dutchie.com/api-2/graphql",
#        "store_id": "61e967a5b39912000c4161a0",
#        "headers": {
#            "accept": "*/*", "apollographql-client-name": "Marketplace (production)", "content-type": "application/json",
#            "referer": "https://ayrdispensaries.com/pennsylvania/state-college/shop/",
#            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15",
#            "x-dutchie-session": "eyJpZCI6Ijg3NDI3YWE0LWQyMjUtNDZkYi1hY2M2LTk2NjU3YmQyMTRjMCIsImV4cGlyZXMiOjE3NjM2MDgwMTE3NzB9"
#        }
#    },
#    "Ayr (Williamsport)": {
#        "api_url": "https://dutchie.com/api-2/graphql",
#        "store_id": "61f14ae1f23793000cc1b771",
#        "headers": {
#            "accept": "*/*", "apollographql-client-name": "Marketplace (production)", "content-type": "application/json",
#            "referer": "https://ayrdispensaries.com/pennsylvania/williamsport/shop/",
#            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15",
#            "x-dutchie-session": "eyJpZCI6Ijg3NDI3YWE0LWQyMjUtNDZkYi1hY2M2LTk2NjU3YmQyMTRjMCIsImV4cGlyZXMiOjE3NjM2MDgwMTE3NzB9"
#        }
#    },
#    "Ayr (Montgomeryville)": {
#        "api_url": "https://dutchie.com/api-2/graphql",
#        "store_id": "62d56a298533b3000c2a2333",
#        "headers": {
#            "accept": "*/*", "apollographql-client-name": "Marketplace (production)", "content-type": "application/json",
#            "referer": "https://ayrdispensaries.com/pennsylvania/montgomeryville/shop/",
#            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15",
#            "x-dutchie-session": "eyJpZCI6Ijg3NDI3YWE0LWQyMjUtNDZkYi1hY2M2LTk2NjU3YmQyMTRjMCIsImV4cGlyZXMiOjE3NjM2MDgwMTE3NzB9"
#        }
#    },
#    "Ayr (DuBois)": {
#        "api_url": "https://dutchie.com/api-2/graphql",
#        "store_id": "63039d99723351000c8f5f8b",
#        "headers": {
#            "accept": "*/*", "apollographql-client-name": "Marketplace (production)", "content-type": "application/json",
#            "referer": "https://ayrdispensaries.com/pennsylvania/dubois/shop/",
#            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15",
#            "x-dutchie-session": "eyJpZCI6Ijg3NDI3YWE0LWQyMjUtNDZkYi1hY2M2LTk2NjU3YmQyMTRjMCIsImV4cGlyZXMiOjE3NjM2MDgwMTE3NzB9"
#        }
#    },
#    "Ayr (Pottsville)": {
#        "api_url": "https://dutchie.com/api-2/graphql",
#        "store_id": "63483984d2b865000c7e2962",
#        "headers": {
#            "accept": "*/*", "apollographql-client-name": "Marketplace (production)", "content-type": "application/json",
#            "referer": "https://ayrdispensaries.com/pennsylvania/pottsville/shop/",
#            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15",
#            "x-dutchie-session": "eyJpZCI6Ijg3NDI3YWE0LWQyMjUtNDZkYi1hY2M2LTk2NjU3YmQyMTRjMCIsImV4cGlyZXMiOjE3NjM2MDgwMTE3NzB9"
#        }
#    },
#    "Ayr (Philadelphia)": {
#        "api_url": "https://dutchie.com/api-2/graphql",
#        "store_id": "6440232231267b0001bc954b",
#        "headers": {
#            "accept": "*/*", "apollographql-client-name": "Marketplace (production)", "content-type": "application/json",
#            "referer": "https://ayrdispensaries.com/pennsylvania/philadelphia/shop/",
#            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15",
#            "x-dutchie-session": "eyJpZCI6Ijg3NDI3YWE0LWQyMjUtNDZkYi1hY2M2LTk2NjU3YmQyMTRjMCIsImV4cGlyZXMiOjE3NjM2MDgwMTE3NzB9"
#        }
#    },
#    "Ayr (Olyphant)": {
#        "api_url": "https://dutchie.com/api-2/graphql",
#        "store_id": "644023a831267b0001bc954d",
#        "headers": {
#            "accept": "*/*", "apollographql-client-name": "Marketplace (production)", "content-type": "application/json",
#            "referer": "https://ayrdispensaries.com/pennsylvania/olyphant/shop/",
#            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15",
#            "x-dutchie-session": "eyJpZCI6Ijg3NDI3YWE0LWQyMjUtNDZkYi1hY2M2LTk2NjU3YmQyMTRjMCIsImV4cGlyZXMiOjE3NjM2MDgwMTE3NzB9"
#        }
#    },
}

def get_all_product_slugs(store_name, store_config):
    """
    Fetches the unique identifiers (`cName` or slugs) for all products in a store.

    This function paginates through the dispensary's menu, collecting the `cName`
    for every product. This is the first step, as these slugs are required to
    fetch the detailed information for each product.

    Args:
        store_name (str): The name of the store being scraped.
        store_config (dict): The configuration dictionary for the store.

    Returns:
        list: A list of dictionaries, each containing a product's `cName` and store info.
    """
    all_products = []
    print(f"Step 1: Fetching product slugs for {store_name}...")
    
    api_url, store_id, headers = store_config['api_url'], store_config['store_id'], store_config['headers']

    page = 0
    while True:
        # This is the GraphQL payload, sent as URL parameters.
        # `extensions.persistedQuery.sha256Hash` is a key part of Dutchie's API.
        # Instead of sending the full query text, the client sends a hash of the query.
        # If this hash ever changes on their backend, this scraper will break.
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
        extensions = {"persistedQuery": {"version": 1, "sha256Hash": "ee29c060826dc41c527e470e9ae502c9b2c169720faa0a9f5d25e1b9a530a4a0"}}
        params = {'operationName': 'FilteredProducts', 'variables': json.dumps(variables), 'extensions': json.dumps(extensions)}

        try:
            response = requests.get(api_url, headers=headers, params=params)
            response.raise_for_status()
            json_response = response.json()

            # Save the raw JSON data
            filename_parts = ['dutchie', store_name, 'products', f'p{page}']
            save_raw_json(json_response, filename_parts)
            if 'errors' in json_response:
                print(f"GraphQL Error in product slugs for {store_name}: {json_response['errors']}")
                break
                
            products = json_response['data']['filteredProducts']['products']
            if not products: break

            for product in products:
                all_products.append({
                    "id": product['id'],  # <-- ADD THIS KEY
                    "cName": product['cName'], "DispensaryID": store_id,
                    "StoreName": store_name, "StoreConfig": store_config
                })
            page += 1
        except requests.exceptions.RequestException as e:
            print(f"Error fetching product slugs for {store_name}: {e}")
            break
        except KeyError:
            print(f"Unexpected JSON structure for {store_name}. Skipping. Response: {response.text}")
            break

    print(f"  ...found {len(all_products)} total product slugs for {store_name}.")
    return all_products

def get_detailed_product_info(product_slugs):
    """
    Fetches and parses detailed information for a list of product slugs.

    This function iterates through the collected slugs, makes a new API call for
    each one to get detailed data (terpenes, cannabinoids, price), and then
    parses the complex JSON response into a simple, flat dictionary.

    Args:
        product_slugs (list): A list of product slug dictionaries from `get_all_product_slugs`.

    Returns:
        list: A list of dictionaries, where each dictionary represents a parsed product.
    """
    all_product_data = []
    print("\nStep 2: Fetching detailed product information from Dutchie...")

    for i, slug_info in enumerate(product_slugs):
        cName, dispensaryId, store_name, store_config = slug_info['cName'], slug_info['DispensaryID'], slug_info['StoreName'], slug_info['StoreConfig']

        if (i + 1) % 50 == 0:
            print(f"  ...processing product {i + 1}/{len(product_slugs)}")

        variables = {
            "includeTerpenes": True, "includeCannabinoids": True, "includeEnterpriseSpecials": False,
            "productsFilter": {
                "cName": cName, "dispensaryId": dispensaryId, "removeProductsBelowOptionThresholds": False,
                "isKioskMenu": False, "bypassKioskThresholds": False, "bypassOnlineThresholds": True, "Status": "All"
            }
        }
        extensions = {"persistedQuery": {"version": 1, "sha256Hash": "47369a02fc8256aaf1ed70d0c958c88514acdf55c5810a5be8e0ee1a19617cda"}}
        params = {'operationName': 'IndividualFilteredProduct', 'variables': json.dumps(variables), 'extensions': json.dumps(extensions)}
        
        try:
            response = requests.get(store_config['api_url'], headers=store_config['headers'], params=params)
            response.raise_for_status()
            json_response = response.json()

            # Save the raw JSON data
            filename_parts = ['dutchie', store_name, 'product_details', cName]
            save_raw_json(json_response, filename_parts)
            if 'errors' in json_response:
                print(f"GraphQL Error in product details for {cName}: {json_response['errors']}")
                continue
                
            products = json_response['data']['filteredProducts']['products']
            if not products: continue

            product = products[0]
            parsed_data = parse_product_details(product, store_name)
            if parsed_data: all_product_data.append(parsed_data)

        except requests.exceptions.RequestException as e:
            print(f"Error fetching details for {cName}: {e}")
        except KeyError:
            print(f"Unexpected JSON structure for details {cName}. Skipping. Response: {response.text}")
            continue

    print(f"  ...successfully parsed {len(all_product_data)} products.")
    return all_product_data

def parse_product_details(product, store_name):
    """
    Parses the complex, nested JSON of a single product into a flat dictionary.

    This function handles the extraction of key information and standardizes it.
    It uses `.get()` extensively to avoid errors if a key is missing in the response.

    Args:
        product (dict): The JSON dictionary for a single product from the API.
        store_name (str): The name of the store.

    Returns:
        dict: A flattened dictionary containing the key product information.
    """
    
    # Standardize category and skip if not in map
    category_name = product.get('type')
    standardized_category = MASTER_CATEGORY_MAP.get(category_name)
    if not standardized_category:
        return None

    # Standardize brand and subcategory
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
    data['Price'] = min(special_prices) if special_prices else (min(prices) if prices else np.nan)

    options = product.get('Options', [])
    weight_str = options[0] if options else None
    data['Weight'] = convert_to_grams(weight_str)
    data['Weight_Str'] = weight_str if weight_str else 'N/A'

    # Process compounds (cannabinoids and terpenes)
    compounds_dict = {}

    # Handle terpenes
    terpenes_list = product.get('terpenes')
    if terpenes_list is not None:
        for item in terpenes_list:
            name = item.get('libraryTerpene', {}).get('name')
            if name:
                standard_name = MASTER_COMPOUND_MAP.get(name)
                if standard_name:
                    compounds_dict[standard_name] = item.get('value')

    # Handle cannabinoids
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

    It calls the other functions in this module to get all product data from
    all configured Dutchie stores and returns it as a single DataFrame.

    Returns:
        pd.DataFrame: A DataFrame containing all scraped product data from Dutchie stores.
    """
    all_store_slugs = []
    for store_name, store_config in DUTCHIE_STORES.items():
        all_store_slugs.extend(get_all_product_slugs(store_name, store_config))

    if not all_store_slugs:
        print("No product slugs found for any Dutchie store. Exiting Dutchie scraper.")
        return pd.DataFrame()
    
    pdb.set_trace()

    product_details = get_detailed_product_info(all_store_slugs)
    if not product_details:
        print("No product data was fetched. Returning an empty DataFrame.")
        return pd.DataFrame()

    df = pd.DataFrame(product_details)
    df['dpg'] = df['Price'] / df['Weight']

    print("\nScraping complete for Dutchie stores. DataFrame created.")
    return df
