import logging
from pathlib import Path
from typing import Optional, Union, List
import pandas as pd
import os

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

EXPECTED_COLS = ["order_id", "user_id", "amount", "ts", "status"]

def transform_transactions(
    input_path: Union[str, Path],
    output_parquet: Optional[Union[str, Path]] = None,
    chunksize: Optional[int] = None
) -> pd.DataFrame:
    input_path = Path(input_path)

    logger.info(f"🔍 Verificando existencia del archivo en: {input_path.resolve()}")
    if not input_path.exists():
        logger.error(f"❌ Archivo no encontrado: {input_path}")
        raise FileNotFoundError(f"No se encontró el archivo: {input_path}")

    _log_file_preview(input_path)

    try:
        if chunksize:
            logger.info(f"📦 Lectura por chunks (chunksize={chunksize})")
            chunks: List[pd.DataFrame] = []
            for raw_chunk in pd.read_csv(input_path, sep=",", chunksize=chunksize, engine="c"):
                chunk = _validate_and_clean(raw_chunk)
                chunks.append(chunk)
            df = pd.concat(chunks, ignore_index=True)
        else:
            logger.info("📥 Lectura completa del archivo CSV")
            raw_df = pd.read_csv(input_path, sep=",", engine="c")
            df = _validate_and_clean(raw_df)

    except pd.errors.ParserError as e:
        logger.error(f"❌ Error de parsing con engine='c'. Reintentando con engine='python'...")
        try:
            raw_df = pd.read_csv(input_path, sep=",", engine="python")
            df = _validate_and_clean(raw_df)
        except Exception as inner:
            logger.exception("❌ Falla al reintentar con engine='python': %s", inner)
            raise
    except Exception as e:
        logger.exception("❌ Error inesperado leyendo el CSV: %s", e)
        raise

    if output_parquet:
        output_path = Path(output_parquet)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_path, index=False)
        logger.info("✅ Parquet guardado en: %s", output_path)

    logger.info("✅ Transformación completa. Filas: %s, Columnas: %s", len(df), df.columns.tolist())
    return df

def _validate_and_clean(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.strip().lower() for c in df.columns]
    logger.info("🔎 Columnas detectadas: %s", df.columns.tolist())

    missing = [c for c in EXPECTED_COLS if c not in df.columns]
    if missing:
        logger.error("❌ Faltan columnas: %s", missing)
        raise ValueError(f"Columnas faltantes: {missing}. Encontradas: {df.columns.tolist()}")

    df["ts"] = pd.to_datetime(df["ts"], errors="coerce")
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df = df.dropna(subset=["order_id", "user_id", "ts", "amount"])
    df["status"] = df["status"].astype(str).str.upper()

    return df[EXPECTED_COLS]

def _log_file_preview(path: Path, n_lines: int = 5) -> None:
    try:
        size = os.path.getsize(path)
        logger.info(f"📄 Tamaño del archivo: {size / 1024:.2f} KB")
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            preview = "".join([next(f) for _ in range(n_lines)])
        logger.info("👀 Primeras líneas del archivo:\n%s", preview)

        if "<html" in preview.lower():
            logger.error("⚠️ El archivo parece HTML. Revisa la URL (¿falta dl=1?)")
            raise ValueError("Archivo parece HTML. Verifica la URL (dl=1).")

    except StopIteration:
        logger.warning("⚠️ Archivo muy corto, menos de %s líneas", n_lines)
    except Exception as e:
        logger.warning("⚠️ No se pudo previsualizar el archivo: %s", e)
