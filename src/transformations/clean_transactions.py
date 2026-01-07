from pathlib import Path
import pandas as pd
import argparse


#Fecha hardcodeada que nos permitira meter un timestamp a la columna time y darle un sentido
FECHA_TS = '2004-11-12 00:00:00'
PROCESSED_BASE = Path('data/processed/parquet')
BASE_TS = pd.Timestamp(FECHA_TS, tz='UTC')

#Fecha del archivo que se quiere procesar
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('--ingestion-date', required=True, help='YYYY-MM-DD (fecha de carpeta raw)')
    return p.parse_args()

def comprobaciones(df: pd.DataFrame) -> None:

    expected_cols = ['Time'] + [f'V{i}' for i in range(1, 29)] + ['Amount', 'Class', 'ingestion_date']
    missing = [c for c in expected_cols if c not in df.columns]
    if missing:
        raise ValueError(f'Faltan columnas: {missing}')
    
    null_counts = df.isna().sum()
    bad_nulls = null_counts[null_counts > 0]
    if not bad_nulls.empty:
        raise ValueError(f'Hay nulos: \n {bad_nulls.to_string()}')
    
    if df['Class'].dtype.kind not in ('i', 'u'):
        raise ValueError('Columna Class tiene que ser de tipo entero')
    
    if (df['Amount'] < 0).any():
        raise ValueError('Columna Amount no puede ser negativa')
    
def build(df: pd.DataFrame) -> pd.DataFrame:

    df = df.drop_duplicates().copy()
   
    df.loc[:,'event_ts'] = BASE_TS + pd.to_timedelta(df['Time'].astype('int64'), unit='s')
    df.loc[:,'event_date'] = df['event_ts'].dt.date

    #Reordenamos las columnas para mayor comprension a simple vista
    cols = ['ingestion_date', 'event_ts', 'event_date', 'Time', 'Amount', 'Class'] + [f'V{i}' for i in range(1,29)]
    return df[cols]

def write(df: pd.DataFrame, ingestion_date: str) -> Path:
    out_dir = PROCESSED_BASE / f'ingestion_date={ingestion_date}'
    out_dir.mkdir(parents=True, exist_ok=True)

    out_file = out_dir / 'transactions_processed.parquet'
    df.to_parquet(out_file, index=False)
    print(f'Creado: {out_file}')
    return out_file

def main() -> None:
    args = parse_args()

    raw_file = Path(f'data/raw/parquet/ingestion_date={args.ingestion_date}/transactions_raw.parquet')
    if not raw_file.exists():
        raise FileNotFoundError(f'Parquet no encontrado en: {raw_file}')
    
    df = pd.read_parquet(raw_file)
    print(f'Archivo leido correctamente: {len(df)}')

    comprobaciones(df)
    print('Comprobaciones del df raw completadas')

    proccesed = build(df)
    print('Build completado correctamente')

    comprobaciones(proccesed.drop(columns=['event_ts', 'event_date']))
    print('Comprobaciones del build completadas')

    write(proccesed, args.ingestion_date)

if __name__ == '__main__':
    main()