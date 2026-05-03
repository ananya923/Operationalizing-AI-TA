"""
Download NYC TLC Yellow Taxi parquet files from the public CloudFront CDN.
Files are saved to data/raw/yellow/.

Usage:
    python notebooks/01_download_data.py
"""

import os
import requests
from pathlib import Path
from tqdm import tqdm

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ROOT = Path(__file__).parent.parent

YELLOW_DIR = ROOT / "data" / "raw" / "yellow"
GREEN_DIR  = ROOT / "data" / "raw" / "green"
YELLOW_DIR.mkdir(parents=True, exist_ok=True)
GREEN_DIR.mkdir(parents=True, exist_ok=True)

# 2023 full year through Feb 2026 (latest available as of April 2026)
YELLOW_MONTHS = [
    f"yellow_tripdata_{year}-{month:02d}.parquet"
    for year, months in {
        2023: range(1, 13),
        2024: range(1, 13),
        2025: range(1, 13),
        2026: range(1, 3),   # Jan + Feb only
    }.items()
    for month in months
]

GREEN_MONTHS = [
    f"green_tripdata_{year}-{month:02d}.parquet"
    for year, months in {
        2023: range(1, 13),
        2024: range(1, 13),
        2025: range(1, 13),
        2026: range(1, 3),
    }.items()
    for month in months
]


def download_file(url: str, dest: Path) -> bool:
    if dest.exists():
        print(f"  [skip] {dest.name} already exists")
        return True
    try:
        r = requests.get(url, stream=True, timeout=60)
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        with open(dest, "wb") as f, tqdm(
            desc=dest.name,
            total=total,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            leave=False,
        ) as bar:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
                bar.update(len(chunk))
        print(f"  [done] {dest.name}")
        return True
    except Exception as e:
        print(f"  [fail] {dest.name}: {e}")
        if dest.exists():
            dest.unlink()
        return False


def main():
    print("=== Downloading Yellow Taxi Data (2023 – Feb 2026) ===")
    for fname in YELLOW_MONTHS:
        url = f"{BASE_URL}/{fname}"
        dest = YELLOW_DIR / fname
        download_file(url, dest)

    print("\n=== Downloading Green Taxi Data (2023 – Feb 2026) ===")
    for fname in GREEN_MONTHS:
        url = f"{BASE_URL}/{fname}"
        dest = GREEN_DIR / fname
        download_file(url, dest)

    print("\n=== Download Complete ===")
    yellow_files = list(YELLOW_DIR.glob("*.parquet"))
    green_files  = list(GREEN_DIR.glob("*.parquet"))
    print(f"Yellow: {len(yellow_files)} files")
    print(f"Green:  {len(green_files)} files")
    total_mb = sum(f.stat().st_size for f in yellow_files + green_files) / 1e6
    print(f"Total size on disk: {total_mb:.0f} MB")


if __name__ == "__main__":
    main()