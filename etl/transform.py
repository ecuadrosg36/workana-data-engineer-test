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

    cols = [
        "transaction_id", "user_id", "timestamp", "amount", "status", "currency"
    ]

    # Leer el archivo en chunks o completo
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

    # Guardar como Parquet si se especifica
    if output_parquet:
        output_path = Path(output_parquet)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_path, index=False)
        logger.info(f"Archivo transformado guardado en: {output_path}")

    return df


def _clean_chunk(df: pd.DataFrame) -> pd.DataFrame:
    """Realiza limpieza básica en un DataFrame de transacciones."""
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["transaction_id", "user_id", "timestamp", "amount"])
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0)
    df["status"] = df["status"].str.upper()
    df["currency"] = df["currency"].str.upper()
    return df
