import logging
import sqlite3
from typing import Optional
import pandas as pd
from etl.transform import transform_transactions
from etl.config import RAW_CSV_PATH, CLEAN_OUTPUT_PATH, SQLITE_DB_PATH

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("etl.load")

def load_dataframe_to_sqlite(
    df: pd.DataFrame,
    sqlite_path: str,
    table_name: str,
    if_exists: str = "append",
    chunksize: int = 50000,
) -> None:
    logger.info("Conectando a SQLite: %s", sqlite_path)
    with sqlite3.connect(sqlite_path) as conn:
        df.to_sql(
            name=table_name,
            con=conn,
            if_exists=if_exists,
            index=False,
            chunksize=chunksize,
        )
        logger.info("✅ Carga completada en la tabla '%s'.", table_name)

def validate_table_not_empty(sqlite_path: str, table_name: str) -> int:
    with sqlite3.connect(sqlite_path) as conn:
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
    if count == 0:
        raise ValueError(f"La tabla '{table_name}' quedó vacía.")
    logger.info("✅ Validación OK. Filas en tabla: %s", count)
    return count

def main(
    table_name: str = "transactions",
    read_from_parquet: bool = False,
    parquet_path: Optional[str] = None,
):
    if read_from_parquet and parquet_path:
        logger.info("Leyendo Parquet: %s", parquet_path)
        df = pd.read_parquet(parquet_path)
    else:
        logger.info("Transformando CSV crudo: %s", RAW_CSV_PATH)
        df = transform_transactions(input_path=RAW_CSV_PATH)

    if df.empty:
        raise ValueError("El DataFrame está vacío. Abortando carga.")

    load_dataframe_to_sqlite(
        df=df,
        sqlite_path=SQLITE_DB_PATH,
        table_name=table_name
    )

    validate_table_not_empty(SQLITE_DB_PATH, table_name)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Cargar datos a SQLite.")
    parser.add_argument("--table", default="transactions")
    parser.add_argument("--from-parquet", dest="parquet_path", default=None)
    args = parser.parse_args()

    main(
        table_name=args.table,
        read_from_parquet=args.parquet_path is not None,
        parquet_path=args.parquet_path,
    )
