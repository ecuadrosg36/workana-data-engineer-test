import logging
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from etl.transform import transform_transactions
from etl.config import RAW_CSV_PATH, CLEAN_OUTPUT_PATH, SQLITE_URL

# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("etl.load")

# ---------------------------------------------------------------------
# SQLite Engine
# ---------------------------------------------------------------------
def get_engine_sqlite() -> Engine:
    logger.info("Creando conexión a SQLite...")
    return create_engine(SQLITE_URL, echo=False)

# ---------------------------------------------------------------------
# Carga y validación
# ---------------------------------------------------------------------
def load_dataframe_to_sqlite(
    df: pd.DataFrame,
    engine: Engine,
    table_name: str,
    if_exists: str = "append",
    chunksize: int = 50_000,
) -> None:
    logger.info(
        "Cargando DataFrame a SQLite (tabla=%s, filas=%s)...",
        table_name,
        len(df),
    )
    df.to_sql(
        name=table_name,
        con=engine,
        if_exists=if_exists,
        index=False,
        method="multi",
        chunksize=chunksize,
    )
    logger.info("Carga finalizada.")

def validate_table_not_empty(engine: Engine, table_name: str) -> int:
    logger.info("Validando que la tabla '%s' no esté vacía...", table_name)
    with engine.connect() as conn:
        count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar_one()
    if count == 0:
        raise ValueError(f"La tabla '{table_name}' quedó vacía.")
    logger.info("Validación OK. Filas en tabla: %s", count)
    return count

# ---------------------------------------------------------------------
# Punto de entrada
# ---------------------------------------------------------------------
def main(
    table_name: str = "transactions",
    read_from_parquet: bool = False,
    parquet_path: Optional[str] = None,
):
    engine = get_engine_sqlite()

    if read_from_parquet and parquet_path:
        logger.info("Leyendo DataFrame desde Parquet: %s", parquet_path)
        df = pd.read_parquet(parquet_path)
    else:
        logger.info("Transformando CSV crudo: %s", RAW_CSV_PATH)
        df = transform_transactions(
            input_path=RAW_CSV_PATH,
            output_parquet=None,
            chunksize=None
        )

    if df.empty:
        raise ValueError("El DataFrame transformado está vacío. Abortando carga.")

    load_dataframe_to_sqlite(
        df=df,
        engine=engine,
        table_name=table_name,
        if_exists="append",
        chunksize=50_000,
    )

    validate_table_not_empty(engine, table_name)
    logger.info("✅ Proceso de carga finalizado con éxito.")

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
        help="Ruta a un parquet para cargar en lugar de transformar el CSV.",
    )

    args = parser.parse_args()

    main(
        table_name=args.table,
        read_from_parquet=args.parquet_path is not None,
        parquet_path=args.parquet_path,
    )
