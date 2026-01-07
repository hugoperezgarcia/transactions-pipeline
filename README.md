# Transactions Data Pipeline (Batch)

End-to-end batch pipeline to ingest a CSV dataset of financial transactions, store it in a raw parquet layer,
clean and enrich it into a processed layer, and build analytics-ready tables (fact/dim + daily metrics).

## Layers
- **Raw** (`data/raw/parquet/ingestion_date=YYYY-MM-DD/`):
  - Source persisted as Parquet
  - Adds `ingestion_date` for lineage/partitioning
- **Processed** (`data/processed/parquet/ingestion_date=YYYY-MM-DD/`):
  - Removes exact duplicates
  - Adds `event_ts` and `event_date` derived from `Time`
  - Basic data quality checks (schema, nulls, amount >= 0, class integer)
- **Analytics** (`data/analytics/parquet/ingestion_date=YYYY-MM-DD/`):
  - `fact_transactions.parquet`
  - `dim_time.parquet`
  - `daily_transaction_metrics.parquet`

## How to run

### 1) Ingest CSV -> Raw Parquet
python3 src/ingestion/ingest_csv.p

### 2) Raw -> Processed
python3 src/transformations/clean_transactions.py --ingestion-date YYYY-MM-DD

### 2) Processed -> Analytics
python3 src/transformations/build_analytics.py --ingestion-date YYYY-MM-DD

