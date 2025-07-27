import pandas as pd
from pathlib import Path

def transform_transactions(input_path: str | Path, output_parquet: str | Path | None = None, chunksize: int | None = None):
    input_path = Path(input_path)
    if chunksize:
        dfs = []
        for chunk in pd.read_csv(input_path, chunksize=chunksize):
            chunk = _clean(chunk)
            dfs.append(chunk)
        df = pd.concat(dfs, ignore_index=True)
    else:
        df = pd.read_csv(input_path)
        df = _clean(df)

    if output_parquet:
        Path(output_parquet).parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_parquet, index=False)
        print(f"✅ Parquet escrito en: {output_parquet}")

    return df

def _clean(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna()
    if 'fecha' in df.columns:
        df['fecha'] = pd.to_datetime(df['fecha'], errors='coerce')
    return df

if __name__ == "__main__":
    transform_transactions("data/sample_transactions.csv", "data/clean/sample_transactions_clean.parquet")
