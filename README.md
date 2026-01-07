# Transactions Data Pipeline (Batch)

End-to-end **Data Engineering batch pipeline** that ingests a CSV of financial transactions, stores it in a raw data lake layer, cleans and enriches it, and produces analytics-ready tables.

The project focuses on **data engineering fundamentals**: layering, reproducibility, data quality, partitioning, and analytics modeling.  
No machine learning is involved.

---

## Architecture

```
CSV
 ↓
Raw (Parquet, partitioned by ingestion_date)
 ↓
Processed (deduplication + event timestamps)
 ↓
Analytics (fact / dim + daily metrics)
```

---

## Dataset
- Credit card transactions (~284k rows)
- `Time` = seconds since first event (no real timestamp)
- Exact duplicate rows present

---

## Data Layers

**Raw**
- CSV → Parquet
- Adds `ingestion_date` for lineage
- No semantic changes

**Processed**
- Removes exact duplicates
- Creates synthetic `event_ts` and `event_date`
- Basic data quality checks (schema, nulls, ranges)

**Analytics**
- `fact_transactions`
- `dim_time`
- `daily_transaction_metrics`

---

## Synthetic Time
Because the dataset has no real timestamps:
```
event_ts = BASE_TS + Time (seconds)
```
This timestamp is **technical**, not business-accurate, and is documented as such.

---

## Run the Pipeline

Make executable (once):
```bash
chmod +x run_pipeline.sh
```

Run with today’s date:
```bash
./run_pipeline.sh
```

Run with a specific ingestion date:
```bash
./run_pipeline.sh YYYY-MM-DD
```

---

## Project Structure

```
.
├── data/ (raw / processed / analytics)
├── notebooks/ (exploration)
├── src/
│   ├── ingestion/
│   └── transformations/
├── run_pipeline.sh
├── requirements.txt
└── README.md
```

---

## Notes
- Parquet used for efficient analytical reads
- Partitioning by `ingestion_date` enables reprocessing
- Pipeline scripts are CLI-parameterized
