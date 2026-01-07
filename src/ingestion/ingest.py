from pathlib import Path
from datetime import date
import pandas as pd

RAW_CSV_PATH = Path('data/raw/csv/creditcard.csv')
RAW_PARQUET_BASE = Path('data/raw/parquet')

def ingest_csv_to_raw_parquet() -> Path:
    if not RAW_CSV_PATH.exists():
        raise FileNotFoundError(f'Missing input file in: {RAW_CSV_PATH}')
    
    df = pd.read_csv(RAW_CSV_PATH)

    expected_cols = ['Time'] + [f'V{i}' for i in range(1, 29)] + ['Amount', 'Class']
    missing = [c for c in expected_cols if c not in df.columns]
    if missing:
        raise ValueError(f'Missing expected columns: {missing}')
    
    #Nueva columna para mayor flexibilidad
    ingest_date = date.today().isoformat()
    df['ingestion_date'] = ingest_date

    #Tambien separamos por carpetas segun la fecha para mayor facilidad/accesibilidad
    out_dir = RAW_PARQUET_BASE / f'ingestion_date={ingest_date}'
    out_dir.mkdir(parents=True, exist_ok=True)

    out_file = out_dir / 'transactions_raw.parquet'
    df.to_parquet(out_file, index=False)

    print(f'Todo bien: Rows leidas -> {len(df):,}')
    print(f'Todo bien: Creado -> {out_file}')
    return out_file

if __name__ == '__main__':
    ingest_csv_to_raw_parquet()
