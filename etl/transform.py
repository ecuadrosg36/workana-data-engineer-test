import pandas as pd
from pathlib import Path
from typing import Union, Optional
import logging

logger = logging.getLogger(__name__)

def transform_transactions(
    input_path: Union[str, Path],
    output_parquet: Optional[Union[str, Path]] = None,
    chunksize: Optional[int] = None
) -> pd.DataFrame:
    """
    Limpia y transforma un archivo CSV de transacciones.

    Args:
        input_path: Ruta al archivo CSV crudo.
        output_parquet: Ruta para guardar el Parquet (opcional).
        chunksize: Tamaño de chunk para lectura (opcional).

    Returns:
        DataFrame limpio.
    """
    input_path = Path(input_path)
    logger.info(f"Iniciando transformación del archivo: {input_path}")

    if not input_path.exists():
        raise FileNotFoundError(f"No se encontró el archivo: {input_path}")

    # Las columnas reales del archivo:
    expected_cols = ["order_id", "user_id", "amount", "ts", "status"]

    try:
        if chunksize:
            logger.info(f"Lectura por chunks de tamaño {chunksize}")
            chunks = []
            for chunk in pd.read_csv(input_path, sep=",", chunksize=chunksize):
                chunk = _validate_and_clean(chunk, expected_cols)
                chunks.append(chunk)
            df = pd.concat(chunks, ignore_index=True)
        else:
            df = pd.read_csv(input_path, sep=",")
            df = _validate_and_clean(df, expected_cols)
    except Exception as e:
        logger.error(f"Error al leer o limpiar el CSV: {e}")
        raise

    if output_parquet:
        output_path = Path(output_parquet)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_path, index=False)
        logger.info(f"Parquet guardado en: {output_path}")

    return df


def _validate_and_clean(df: pd.DataFrame, expected_cols: list) -> pd.DataFrame:
    """Valida columnas y aplica limpieza básica"""
    actual_cols = df.columns.tolist()
    logger.info(f"Columnas detectadas: {actual_cols}")

    missing = [col for col in expected_cols if col not in actual_cols]
    if missing:
        raise ValueError(f"Faltan columnas requeridas: {missing}")

    df["ts"] = pd.to_datetime(df["ts"], errors="coerce")
    df = df.dropna(subset=["order_id", "user_id", "ts", "amount"])
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0)
    df["status"] = df["status"].str.upper()

    return df
