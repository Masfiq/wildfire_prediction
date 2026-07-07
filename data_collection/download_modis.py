"""
Download MODIS MCD64A1 burned area product for Western USA (2018-2023).
Requires a NASA Earthdata account: https://urs.earthdata.nasa.gov/
"""

import earthaccess
import os
import time
from pathlib import Path

MAX_RETRIES = 5
RETRY_DELAY_SEC = 30  # NASA's servers occasionally 502 — back off and retry rather than dying


########################edit these before runnning

years = range(2023, 2024)   

bounding_box = (-124.5, 32.5, -114.0, 49.0)
# Western USA: California, Oregon, Washington
 # (west, south, east, north)

OUTPUT_DIR = Path("/s/chopin/e/proj/hyperspec/masfiq/wildfire_prediction/dataset/modis/MCD64A1")

##################################

# --- Config ---



# MCD64A1 breaks down as:

# M — MODIS
# C — Combined (both Terra and Aqua satellites, not just one)
# D — Daily input (even though the final product is monthly)
# 64 — the product number for Burned Area
# A — first version of this product family
# 1 — 1 month compositing period
# So in plain English: MODIS Combined Burned Area Monthly Product

# It tells you which 500m pixels burned each month, and on what day the fire was detected. It goes back to 2000, covers the whole globe, and is the standard ground truth used in most wildfire research papers.

# The other MODIS fire products for reference:

# MOD14 — active fire detection (Terra, daily, 1km) — detects ongoing fires
# MYD14 — same but from Aqua satellite
# MCD14ML — combined active fire point locations
# MOD13A2 — vegetation indices (NDVI/EVI), useful as input features
# For your use case (ground truth labels for where fire occurred), MCD64A1 is the right one.


YEARS = years        # 2018 to 2023 inclusive
# Western USA: California, Oregon, Washington
BOUNDING_BOX = bounding_box  # (west, south, east, north)

# earthaccess.download() parallelizes with a thread pool internally
THREADS = int(os.environ.get("SLURM_CPUS_PER_TASK", "8"))

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def download_modis_burned_area():
    # Login — reads from ~/.netrc if already configured, otherwise prompts
    earthaccess.login(persist=True)

    for year in YEARS:
        year_dir = OUTPUT_DIR / str(year)
        year_dir.mkdir(exist_ok=True)

        temporal = (f"{year}-01-01", f"{year}-12-31")
        print(f"\nSearching MCD64A1 for {year}...")

        results = earthaccess.search_data(
            short_name="MCD64A1",
            version="061",
            bounding_box=BOUNDING_BOX,
            temporal=temporal,
            # count=100,
        )

#         MODIS data is released in Collections — NASA's term for reprocessing versions where they improve the algorithm and reprocess the entire archive.

# The versions are:

# Collection 5 (005) — old, retired
# Collection 6 (006) — previous standard
# Collection 6.1 (061) — current standard (released ~2022, fixes calibration issues)

# count=100 limits the search to return at most 100 granules per year.

# For MCD64A1, Western USA covers roughly 2 MODIS tiles × 12 months = 24 granules per year — so 100 is just a safe upper bound to make sure we get everything without hitting pagination.


        if not results:
            print(f"  No results found for {year}")
            continue

        print(f"  Found {len(results)} granules — downloading to {year_dir} with {THREADS} threads")

        # earthaccess.download() skips files that already exist locally, so
        # a retry only re-fetches whatever failed/is missing, not everything.
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                earthaccess.download(results, local_path=str(year_dir), threads=THREADS)
                break
            except Exception as e:
                print(f"  Attempt {attempt}/{MAX_RETRIES} failed for {year}: {e}")
                if attempt == MAX_RETRIES:
                    print(f"  Giving up on {year} after {MAX_RETRIES} attempts")
                else:
                    time.sleep(RETRY_DELAY_SEC)

    print("\nDone. Files saved to:", OUTPUT_DIR.resolve())


if __name__ == "__main__":
    download_modis_burned_area()
