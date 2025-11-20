# Standardization Maps
# -----------------------------------------------------------------------------
# This document serves as the "Truth Source" for how we standardize messy data.
#
# When scrapers find a product, the data is often inconsistent (e.g., one store
# says "Cresco" and another says "Cresco Labs"). This file lists the rules
# we use to convert all those variations into a single, clean format.
#
# These maps are implemented in `scrapers/scraper_utils.py`.
# -----------------------------------------------------------------------------

## 🛍️ Brand Map

**Strategy:** Canonical Brand. All variations are mapped to their primary brand name.

* `& Shine`, `&Shine` -> **"&Shine"**
* `Cresco`, `Cresco™` -> **"Cresco"**
* `Doctor Solomon's`, `Dr. Solomon's` -> **"Doctor Solomon's"**
* `FIND`, `Find`, `Find.` -> **"Find"**
* `FloraCal`, `FloraCal Farms` -> **"FloraCal Farms"**
* `Garcia`, `Garcia Hand Picked` -> **"Garcia Hand Picked"**
* `Maitri`, `Maitri Genetics`, `Maitri Medicinals` -> **"Maitri"**
* `Modern Flower`, `Modern Flower Ground` -> **"Modern Flower"**
* `mood`, `mood by Vytal`, `Mood by Vytal` -> **"mood"**
* `Penn Health`, `Penn Health Group`, `PHG`, `PhG` -> **"PHG"**
* `Prime`, `Prime Wellness` -> **"Prime"**
* `R.O.`, `R.O. Ground`, `R.O. Shake` -> **"R.O."**
* `RYTHM`, `Rythm` -> **"Rythm"**
* `SeCHe`, `Seche` -> **"Seche"**
* `Select`, `Select Briq`, `Select X` -> **"Select"**
* `Solventless by Vytal`, `Vytal Solventless`, `Solventless` -> **"Vytal Solventless"**
* `Strane`, `Strane Reserve`, `Strane Stash` -> **"Strane"**
* `Sunshine`, `Sunshine Cannabis` -> **"Sunshine"**
* `Supply/Cresco` -> **"Supply"**
* `Vytal`, `Vytal Options` -> **"Vytal"**
* *(...and all other unique brands mapped to themselves)*

---

## 🗂️ Category Map

**Strategy:** Only map medication-related categories. Non-medication types (like `Accessories` or `Apparel`) are **ignored** and not scraped.

* `CONCENTRATE`, `Concentrate`, `Concentrates`, `extract` -> **"Concentrates"**
* `EDIBLE`, `Edible`, `Edibles` -> **"Edibles"**
* `FLOWER`, `Flower` -> **"Flower"**
* `ORALS`, `Oral` -> **"Orals"**
* `TINCTURES`, `Tincture` -> **"Tinctures"**
* `TOPICALS`, `Topicals` -> **"Topicals"**
* `Vaporizers`, `vape`, `vapes` -> **"Vaporizers"**

---

## 🗂️ Subcategory Map

**Strategy:** Map granular variations to simple, high-level subcategories.

### Flower Subtypes
* `WHOLE_FLOWER`, `Flower`, `Premium Flower`, `premium`, `Bud` -> **"Flower"**
* `smalls`, `SMALL_BUDS`, `Popcorn`, `Mini Buds` -> **"Small Buds"**
* `SHAKE_TRIM`, `shake`, `Ground Flower`, `PRE_GROUND` -> **"Ground/Shake"**

### Vaporizer Subtypes
* `disposables`, `disposable_pen` -> **"Cartridge"** (Note: We currently group disposables into cartridges for broader analysis, or map them separately if desired)
* `CARTRIDGES`, `cartridge`, `cured-resin-cartridge`, `live-resin-cartridge`, `pods` -> **"Cartridge"**

### Concentrate Subtypes
* `LIVE_RESIN`, `Live Resin`, `live_resin` -> **"Live Resin"**
* `ROSIN`, `Rosin`, `rosin` -> **"Rosin"**
* `RSO`, `rso` -> **"RSO"**
* `SHATTER`, `shatter` -> **"Shatter"**
* `SUGAR`, `sugar` -> **"Sugar"**
* `BADDER`, `badder` -> **"Badder"**
* `BUDDER`, `budder` -> **"Budder"**
* `CRUMBLE`, `crumble` -> **"Crumble"**
* `WAX`, `wax` -> **"Wax"**
* `KIEF`, `kief` -> **"Kief"**

---

## 🧪 Compound Maps (Terpenes & Cannabinoids)

**Strategy:** Fix typos and standardize chemical names.

### Cannabinoid Map
* `"TAC\" - Total Active Cannabinoids"`, `TAC` -> **"TAC"**
* `CBD` -> **"CBD"**
* `CBDA`, `CBDA (Cannabidiolic acid)` -> **"CBDa"**
* `CBG`, `CBG (Cannabigerol)` -> **"CBG"**
* `CBGA`, `CBGA (Cannabigerolic acid)` -> **"CBGa"**
* `CBN` -> **"CBN"**
* `d8-THC`, `Delta-8 THC` -> **"Delta-8 THC"**
* `THC`, `THC-D9 (Delta 9–tetrahydrocannabinol)` -> **"THC"**
* `THCA`, `THCA (Δ9-tetrahydrocannabinolic acid)` -> **"THCa"**
* `THCV`, `thcv` -> **"THCv"**

### Terpene Map
* `a_terpinene`, `alpha-Terpinene` -> **"alpha-Terpinene"**
* `alpha-Bisabolol`, `Bisabolol` -> **"alpha-Bisabolol"**
* `b_caryophyllene`, `Beta Caryophyllene`, `Caryophyllene`, `CARYOPHYLLENE` -> **"beta-Caryophyllene"**
* `b_myrcene`, `beta-Myrcene`, `BetaMyrcene`, `Myrcene`, `MYRCENE` -> **"beta-Myrcene"**
* `Camphene` -> **"Camphene"**
* `carene`, `Carene` -> **"Carene"**
* `caryophyllene_oxide`, `CaryophylleneOxide` -> **"Caryophyllene Oxide"**
* `Eucalyptol` -> **"Eucalyptol"**
* `Farnesene` -> **"Farnesene"**
* `Geraniol` -> **"Geraniol"**
* `Guaiol` -> **"Guaiol"**
* `Humulene`, `HUMULENE` -> **"Humulene"**
* `Limonene`, `LIMONENE` -> **"Limonene"**
* `Linalool`, `LINALOOL` -> **"Linalool"**
* `Ocimene` -> **"Ocimene"**
* `p_cymene` -> **"p-Cymene"**
* `Terpineol` -> **"Terpineol"**
* `Terpinolene` -> **"Terpinolene"**
* `trans_nerolidal`, `trans-nerolidol`, `Nerolidol` -> **"trans-Nerolidol"**
* `y_terpinene` -> **"gamma-Terpinene"**

### Special Aggregation Rules

* **Pinene:** In the final analysis (`analysis.py`), `alpha-Pinene` and `beta-Pinene` are **summed** together into a single **"Pinene"** column to simplify the visualizations.
