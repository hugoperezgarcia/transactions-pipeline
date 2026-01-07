from pathlib import Path
import pandas as pd
import argparse

ANALYTICS_BASE = Path('data/analytics/parquet')

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('--ingestion-date', required=True, help='YYY-MM-DD (fecha de carpeta proceessed)')
    return p.parse_args()

def fact_transactions(df: pd.DataFrame) -> pd.DataFrame:
    fact_cols = ['ingestion_date', 'event_ts', 'event_date', 'Amount', 'Class']
    fact = df.loc[:, fact_cols].copy()
    fact = fact.sort_values(
        ['event_ts', 'ingestion_date', 'Amount', 'Class'],
        kind='mergesort'
    ).reset_index(drop=True)

    #Columna clave tecnica surrogate key, para identificar filas en analytics
    fact['transactions_sk'] = range(1, len(fact) + 1)
    fact = fact[['transactions_sk'] + fact_cols]

    return fact

def dim_time(df: pd.DataFrame) -> pd.DataFrame:
    dim = pd.DataFrame({'event_date': pd.to_datetime(df['event_date']).drop_duplicates().sort_values()})
    dim['year'] = dim['event_date'].dt.year
    dim['month'] = dim['event_date'].dt.month
    dim['day'] = dim['event_date'].dt.day
    dim['day_of_week'] = dim['event_date'].dt.dayofweek
    dim['is_weekend'] = dim['day_of_week'] >= 5
    return dim

def daily_metrics(df: pd.DataFrame) -> pd.DataFrame:
    g = df.groupby('event_date', as_index=False).agg(
        transactions=('Amount', 'size'),
        total_ammount=('Amount', 'sum'),
        avg_amount=('Amount', 'mean'),
        fraud_transactions=('Class', 'sum'),   
    )
    g['fraud_rate'] = g['fraud_transactions'] / g['transactions']
    return g.sort_values('event_date')

def write(df: pd.DataFrame, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False)
    print(f'Creado en: {out_path}')

def main() -> None:
    args = parse_args()
    processed_file = Path(f'data/processed/parquet/ingestion_date={args.ingestion_date}/transactions_processed.parquet')

    if not processed_file.exists():
        raise FileNotFoundError(f'No se ha encontrado el archivo en: {processed_file}')
    
    df = pd.read_parquet(processed_file)
    print('Archivo leido correctamente')
    
    base_dir = ANALYTICS_BASE / f'ingestion_date={args.ingestion_date}'

    fact = fact_transactions(df)
    dim = dim_time(df)
    daily = daily_metrics(df)

    write(fact, base_dir / 'fact_transactions.parquet')
    write(dim, base_dir / 'dim_time.parquet')
    write(daily, base_dir / 'daily_transactions_metrics.parquet')

if __name__ == '__main__':
    main()