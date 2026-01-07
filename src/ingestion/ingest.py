from pathlib import Path
from datetime import date
import argparse
import zipfile
import pandas as pd

RAW_CSV_PATH = Path('data/raw/csv/creditcard.csv')
RAW_ZIP_PATH = Path('data/raw/csv/creditcard.zip')
RAW_PARQUET_BASE = Path('data/raw/parquet')

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('--ingestion-date', help='YYYY-MM-DD (fecha de ingesta)')
    return p.parse_args()

def ensure_raw_csv() -> None:
    if RAW_CSV_PATH.exists():
        return
    if RAW_ZIP_PATH.exists():
        with zipfile.ZipFile(RAW_ZIP_PATH, 'r') as zf:
            zf.extractall(RAW_CSV_PATH.parent)
    if not RAW_CSV_PATH.exists():
        raise FileNotFoundError(f'Missing input file in: {RAW_CSV_PATH}')

def ingest_csv_to_raw_parquet(ingestion_date: str) -> Path:
    ensure_raw_csv()
    
    df = pd.read_csv(RAW_CSV_PATH)

    expected_cols = ['Time'] + [f'V{i}' for i in range(1, 29)] + ['Amount', 'Class']
    missing = [c for c in expected_cols if c not in df.columns]
    if missing:
        raise ValueError(f'Missing expected columns: {missing}')
    
    #Nueva columna para mayor flexibilidad
    df['ingestion_date'] = ingestion_date

    #Tambien separamos por carpetas segun la fecha para mayor facilidad/accesibilidad
    out_dir = RAW_PARQUET_BASE / f'ingestion_date={ingestion_date}'
    out_dir.mkdir(parents=True, exist_ok=True)

    out_file = out_dir / 'transactions_raw.parquet'
    df.to_parquet(out_file, index=False)

    print(f'Todo bien: Rows leidas -> {len(df):,}')
    print(f'Todo bien: Creado -> {out_file}')
    return out_file

if __name__ == '__main__':
    args = parse_args()
    ingestion_date = args.ingestion_date or date.today().isoformat()
    ingest_csv_to_raw_parquet(ingestion_date)
