import logging
from pathlib import Path
from typing import Optional, Union, List

import pandas as pd

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


EXPECTED_COLS = ["order_id", "user_id", "amount", "ts", "status"]


def transform_transactions(
    input_path: Union[str, Path],
    output_parquet: Optional[Union[str, Path]] = None,
    chunksize: Optional[int] = None
) -> pd.DataFrame:
    """
    Limpia y transforma el CSV de transacciones:
      - Valida que el archivo exista y no sea HTML.
      - Estandariza columnas a minúsculas.
      - Verifica presencia de columnas esperadas.
      - Parsea fechas, convierte numéricos y normaliza status.
      - (Opcional) Guarda a Parquet.

    Args:
        input_path: Ruta al CSV crudo.
        output_parquet: Ruta a Parquet de salida (opcional).
        chunksize: Tamaño de chunk para lectura en streaming.

    Returns:
        DataFrame limpio.
    """
    input_path = Path(input_path)
    if not input_path.exists():
        raise FileNotFoundError(f"No se encontró el archivo: {input_path}")

    _log_file_preview(input_path)

    try:
        if chunksize:
            logger.info("Lectura por chunks (chunksize=%s)", chunksize)
            chunks: List[pd.DataFrame] = []
            for raw_chunk in pd.read_csv(
                input_path,
                sep=",",
                chunksize=chunksize,
                engine="c",  # si da error, cambia a "python"
            ):
                chunk = _validate_and_clean(raw_chunk)
                chunks.append(chunk)
            df = pd.concat(chunks, ignore_index=True)
        else:
            logger.info("Lectura completa del archivo")
            raw_df = pd.read_csv(
                input_path,
                sep=",",
                engine="c",  # si vuelve a fallar, prueba con "python"
            )
            df = _validate_and_clean(raw_df)
    except pd.errors.ParserError as e:
        logger.error(
            "ParserError leyendo el CSV. Es muy probable que el archivo no sea CSV válido "
            "(¿Dropbox sin dl=1?) o tenga separadores inconsistentes. Error: %s", e
        )
        raise
    except Exception as e:
        logger.error("Error al leer o limpiar el CSV: %s", e)
        raise

    if output_parquet:
        output_path = Path(output_parquet)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_path, index=False)
        logger.info("Parquet guardado en: %s", output_path)

    logger.info("Transformación finalizada. Filas: %s, Columnas: %s", len(df), list(df.columns))
    return df


def _validate_and_clean(df: pd.DataFrame) -> pd.DataFrame:
    """Valida columnas y aplica limpieza básica."""
    # Estándar: bajar columnas a minúsculas
    df.columns = [c.strip().lower() for c in df.columns]
    logger.info("Columnas detectadas (normalizadas): %s", df.columns.tolist())

    # Validar columnas mínimas requeridas
    missing = [c for c in EXPECTED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas requeridas en el CSV: {missing}. Columnas presentes: {df.columns.tolist()}")

    # Limpieza
    df["ts"] = pd.to_datetime(df["ts"], errors="coerce")
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df = df.dropna(subset=["order_id", "user_id", "ts", "amount"])

    # Normalizaciones
    df["status"] = df["status"].astype(str).str.upper()

    # Ordenar columnas (opcional)
    df = df[EXPECTED_COLS]

    return df


def _log_file_preview(path: Path, n_lines: int = 5) -> None:
    """Loggea tamaño del archivo y las primeras líneas para depurar problemas de formato."""
    try:
        size = path.stat().st_size
        logger.info("Archivo a transformar: %s (%.2f KB)", path, size / 1024)

        with path.open("r", encoding="utf-8", errors="ignore") as f:
            preview = "".join([next(f) for _ in range(n_lines)])
        logger.info("Primeras %s líneas del archivo:\n%s", n_lines, preview)

        # Heurística: si parece HTML, avisar
        if "<html" in preview.lower():
            logger.error(
                "El archivo parece ser HTML. Probablemente la URL de Dropbox no usa dl=1. "
                "Revisa CSV_URL en etl/config.py."
            )
            raise ValueError("Archivo no válido (parece HTML). Verifica la URL de descarga (dl=1).")

    except StopIteration:
        logger.warning("El archivo tiene menos de %s líneas.", n_lines)
    except Exception as e:
        logger.warning("No se pudo previsualizar el archivo: %s", e)
