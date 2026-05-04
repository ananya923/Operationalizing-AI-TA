#!/bin/bash

set -e

if [ -z "$1" ]; then
  echo "Usage: $0 <GCS_BUCKET>"
  echo "Example: $0 gs://your-gcs-bucket"
  exit 1
fi

GCS_BUCKET="$1"
echo "Downloading data from $GCS_BUCKET..."

mkdir -p /data/processed

echo "  → demand_enriched.parquet..."
gcloud storage cp "$GCS_BUCKET/demand_enriched.parquet" /data/processed/

echo "  → lgbm_demand_model.txt..."
gcloud storage cp "$GCS_BUCKET/lgbm_demand_model.txt" /data/processed/

if [ -f "../data/processed/demand_enriched.parquet" ] && [ -f "../data/processed/lgbm_demand_model.txt" ]; then
  echo "✓ Download complete!"
  ls -lh /data/processed/
else
  echo "✗ Download failed"
  exit 1
fi