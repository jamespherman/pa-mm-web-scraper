# Standardization Maps

This document contains the finalized, canonical mappings for standardizing raw data from all scrapers. These maps are based on the `UNIQUE_VALUE_REPORT.md` and are used to build the parsers.

---

## 🛍️ Brand Map

**Strategy:** Option B (Canonical Brand). All variations are mapped to their primary brand name, not their parent company.

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

**Strategy:** Only map medication-related categories. Non-medication types (`Accessories`, `Apparel`, `Gear`, `PRE_ROLLS`, `Pre-Rolls`, `SEEDS`) will be **ignored** and will not be parsed.

* `CONCENTRATE`, `Concentrate`, `Concentrates` -> **"Concentrates"**
* `EDIBLE`, `Edible`, `Edibles` -> **"Edibles"**
* `FLOWER`, `Flower` -> **"Flower"**
* `ORALS`, `Oral` -> **"Orals"**
* `TINCTURES`, `Tincture` -> **"Tinctures"**
* `TOPICALS`, `Topicals` -> **"Topicals"**
* `Vaporizers`, `vape` -> **"Vaporizers"**

---

## 🗂️ Subcategory Map

**Strategy:** Map all granular variations to simple, high-level subcategories based on your rules.

### Flower Subtypes
* `WHOLE_FLOWER`, `Flower`, `Premium Flower`, `premium`, `Bud` -> **"Flower"**
* `smalls`, `SMALL_BUDS`, `Popcorn`, `Mini Buds` -> **"Small Buds"**
* `SHAKE_TRIM`, `shake` -> **"Shake"**
* `Ground Flower`, `PRE_GROUND` -> **"Ground"**

### Vaporizer Subtypes
* `disposables`, `disposable_pen` -> **"Disposables"**
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
* *(...and so on for other concentrate types)*

---

## 🧪 Compound Maps (Terpenes & Cannabinoids)

**Strategy:** All junk strings (e.g., `"Description courtesy of JaneTHC"`) will be **ignored and discarded**. Most compounds are a 1:1 mapping.

### Cannabinoid Map
* `"TAC\" - Total Active Cannabinoids"` -> **"TAC"**
* `CBD` -> **"CBD"**
* `CBDA`, `CBDA (Cannabidiolic acid)` -> **"CBDa"**
* `CBG`, `CBG (Cannabigerol)` -> **"CBG"**
* `CBGA`, `CBGA (Cannabigerolic acid)` -> **"CBGa"**
* `CBN` -> **"CBN"**
* `d8-THC` -> **"Delta-8 THC"**
* `THC`, `THC-D9 (Delta 9–tetrahydrocannabinol)` -> **"THC"**
* `THCA`, `THCA (Δ9-tetrahydrocannabinolic acid)` -> **"THCa"**
* `THCV`, `thcv` -> **"THCv"**

### Terpene Map
* `a_terpinene` -> **"alpha-Terpinene"**
* `alpha-Bisabolol`, `Bisabolol` -> **"alpha-Bisabolol"**
* `b_caryophyllene`, `Beta Caryophyllene`, `Caryophyllene`, `CARYOPHYLLENE` -> **"beta-Caryophyllene"**
* `b_myrcene`, `beta-Myrcene`, `BetaMyrcene`, `Myrcene`, `MYRCENE` -> **"beta-Myrcene"**
* `Camphene` -> **"Camphene"**
* `carene` -> **"Carene"**
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
* `trans_nerolidal`, `trans-nerolidol` -> **"trans-Nerolidol"**
* `y_terpinene` -> **"gamma-Terpinene"**

### Special Aggregation Rules

* **Pinene:** All Pinene-related keys must be **summed** and stored under the single key **"Pinene"**.
    * **Source Keys:** `a-Pinene`, `alpha-Pinene`, `b_pinene`, `beta-Pinene`, `BetaPinene`, `Pinene`, `PINENE`
    * **Target Key:** `Pinene`
