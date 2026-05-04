#!/usr/bin/env python3
import os
import sys
from google.cloud import storage

bucket_name = os.environ.get("GCS_BUCKET", sys.argv[1] if len(sys.argv) > 1 else None)
if not bucket_name:
    print("Error: GCS_BUCKET not set")
    sys.exit(1)

# Strip gs:// if present
bucket_name = bucket_name.replace("gs://", "")

print(f"Downloading data from gs://{bucket_name}...")
client = storage.Client()
bucket = client.bucket(bucket_name)

os.makedirs("/data/processed", exist_ok=True)

for filename in ["demand_enriched.parquet", "lgbm_demand_model.txt"]:
    print(f"  → {filename}...")
    bucket.blob(filename).download_to_filename(f"/data/processed/{filename}")

print("✓ Download complete!")
