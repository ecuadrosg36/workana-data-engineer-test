import logging
import sqlite3
from typing import Optional
import pandas as pd
from etl.transform import transform_transactions
from etl.config import RAW_CSV_PATH, CLEAN_OUTPUT_PATH, SQLITE_DB_PATH

# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("etl.load")

# ---------------------------------------------------------------------
# Carga con sqlite3
# ---------------------------------------------------------------------
def load_dataframe_to_sqlite(
    df: pd.DataFrame,
    table_name: str,
    if_exists: str = "append",
    chunksize: int = 50_000,
) -> None:
    logger.info("Conectando a SQLite: %s", SQLITE_DB_PATH)
    with sqlite3.connect(SQLITE_DB_PATH) as conn:
        df.to_sql(
            name=table_name,
            con=conn,
            if_exists=if_exists,
            index=False,
            chunksize=chunksize,
        )
        logger.info("✅ Carga completada en la tabla '%s'.", table_name)

def validate_table_not_empty(table_name: str) -> int:
    with sqlite3.connect(SQLITE_DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
    if count == 0:
        raise ValueError(f"La tabla '{table_name}' quedó vacía.")
    logger.info("✅ Validación OK. Filas en tabla: %s", count)
    return count

# ---------------------------------------------------------------------
# Punto de entrada
# ---------------------------------------------------------------------
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
        df = transform_transactions(
            input_path=RAW_CSV_PATH,
            output_parquet=None,
            chunksize=None
        )

    if df.empty:
        raise ValueError("El DataFrame está vacío. Abortando carga.")

    load_dataframe_to_sqlite(
        df=df,
        table_name=table_name,
        if_exists="append",
        chunksize=50_000,
    )

    validate_table_not_empty(table_name)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Cargar datos a SQLite.")
    parser.add_argument(
        "--table",
        default="transactions",
        help="Nombre de la tabla destino (default: transactions)",
    )
    parser.add_argument(
        "--from-parquet",
        dest="parquet_path",
        default=None,
        help="Ruta a un Parquet en lugar de transformar el CSV.",
    )

    args = parser.parse_args()

    main(
        table_name=args.table,
        read_from_parquet=args.parquet_path is not None,
        parquet_path=args.parquet_path,
    )
