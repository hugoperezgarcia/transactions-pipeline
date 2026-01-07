#!/usr/bin/env bash
set -euo pipefail

# -----------------------------
# Config
# -----------------------------
INGESTION_DATE="${1:-$(date +%F)}"

echo "===================================="
echo "Running Transactions Data Pipeline"
echo "Ingestion date: ${INGESTION_DATE}"
echo "===================================="

# -----------------------------
# Activate venv if exists
# -----------------------------
if [ -d "venv" ]; then
  echo "[INFO] Activating virtual environment"
  source venv/bin/activate
fi

# -----------------------------
# Step 1: Ingest CSV -> Raw
# -----------------------------
echo "[STEP 1] Ingest CSV -> Raw Parquet"
python3 src/ingestion/ingest.py \
  --ingestion-date "${INGESTION_DATE}"

# -----------------------------
# Step 2: Raw -> Processed
# -----------------------------
echo "[STEP 2] Build Processed Layer"
python3 src/transformations/clean_transactions.py \
  --ingestion-date "${INGESTION_DATE}"

# -----------------------------
# Step 3: Processed -> Analytics
# -----------------------------
echo "[STEP 3] Build Analytics Layer"
python3 src/transformations/build_analytics.py \
  --ingestion-date "${INGESTION_DATE}"

echo "===================================="
echo "Pipeline finished correctly"
echo "===================================="
