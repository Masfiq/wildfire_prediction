"""
Convert MODIS MCD64A1 HDF4 files to GeoTIFF.
Extracts the Burn Date layer and reprojects to WGS84 (EPSG:4326).
"""

import os
import subprocess
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from tqdm import tqdm

INPUT_DIR  = Path("/s/chopin/e/proj/hyperspec/masfiq/wildfire_prediction/dataset/modis/MCD64A1")
OUTPUT_DIR = Path("/s/chopin/e/proj/hyperspec/masfiq/wildfire_prediction/dataset/modis/geotiff")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

LAYER = "Burn_Date"   # the layer inside MCD64A1 we care about

MAX_WORKERS = int(os.environ.get("SLURM_CPUS_PER_TASK", "8"))


def convert(hdf_path: Path):
    # GDAL sees HDF4 layers as: HDF4_EOS:EOS_GRID:"file.hdf":MOD_Grid_Monthly_500m_DB_BA:Burn_Date
    subdataset = f'HDF4_EOS:EOS_GRID:"{hdf_path}":MOD_Grid_Monthly_500m_DB_BA:{LAYER}'

    # Step 1: extract layer to a temp sinusoidal GeoTIFF
    temp_tif  = OUTPUT_DIR / (hdf_path.stem + "_sinu.tif")
    final_tif = OUTPUT_DIR / (hdf_path.stem + ".tif")

    subprocess.run([
        "gdal_translate", "-of", "GTiff", subdataset, str(temp_tif)
    ], check=True)

    # Step 2: reproject from MODIS sinusoidal to WGS84
    subprocess.run([
        "gdalwarp",
        "-s_srs", "+proj=sinu +R=6371007.181 +nadgrids=@null +wktext",
        "-t_srs", "EPSG:4326",
        "-r", "near",
        "-of", "GTiff",
        str(temp_tif), str(final_tif)
    ], check=True)

    temp_tif.unlink()  # delete intermediate file
    return final_tif.name


def main():
    hdf_files = sorted(INPUT_DIR.rglob("*.hdf"))
    if not hdf_files:
        print("No HDF files found in", INPUT_DIR)
        return

    print(f"Converting {len(hdf_files)} HDF files using {MAX_WORKERS} workers...")

    ctx = mp.get_context("spawn")
    with ProcessPoolExecutor(max_workers=MAX_WORKERS, mp_context=ctx) as ex:
        for name in tqdm(ex.map(convert, hdf_files), total=len(hdf_files)):
            print(f"  Saved: {name}")

    print("\nDone. GeoTIFFs saved to:", OUTPUT_DIR.resolve())


if __name__ == "__main__":
    main()
