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
        input_path (str | Path): Ruta al archivo CSV crudo.
        output_parquet (str | Path, optional): Ruta donde guardar el DataFrame como parquet.
        chunksize (int, optional): Tamaño de chunk para lectura eficiente.

    Returns:
        pd.DataFrame: DataFrame limpio y transformado.
    """
    input_path = Path(input_path)

    logger.info(f"Iniciando transformación del archivo: {input_path}")
    if not input_path.exists():
        raise FileNotFoundError(f"No se encontró el archivo: {input_path}")

    cols = ["order_id", "user_id", "amount", "ts", "status"]

    if chunksize:
        logger.info(f"Lectura por chunks (chunksize={chunksize})")
        chunks = []
        for chunk in pd.read_csv(input_path, usecols=cols, chunksize=chunksize):
            cleaned = _clean_chunk(chunk)
            chunks.append(cleaned)
        df = pd.concat(chunks, ignore_index=True)
    else:
        logger.info("Lectura completa del archivo")
        df = pd.read_csv(input_path, usecols=cols)
        df = _clean_chunk(df)

    if output_parquet:
        output_path = Path(output_parquet)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_path, index=False)
        logger.info(f"Archivo transformado guardado en: {output_path}")

    return df


def _clean_chunk(df: pd.DataFrame) -> pd.DataFrame:
    """Realiza limpieza básica en un DataFrame de transacciones."""
    df["ts"] = pd.to_datetime(df["ts"], errors="coerce")
    df = df.dropna(subset=["order_id", "user_id", "ts", "amount"])
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0)
    df["status"] = df["status"].str.upper()
    return df
