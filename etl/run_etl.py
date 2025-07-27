import logging
from pathlib import Path

from etl.config import (
    CSV_URL,
    RAW_CSV_PATH,
    CLEAN_OUTPUT_PATH,
    MIN_SIZE_BYTES,
    TIMEOUT_SECONDS,
)
from scripts.download_csv import download_csv
from etl.sensors import wait_for_file
from etl.transform import transform_transactions

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)

logger = logging.getLogger("run_etl")


def main():
    logger.info("Iniciando ETL...")

    # 1) Descargar
    logger.info("Descargando CSV...")
    download_csv(CSV_URL, str(RAW_CSV_PATH))

    # 2) Sensor de archivo
    logger.info("Esperando archivo...")
    wait_for_file(str(RAW_CSV_PATH), min_size_bytes=MIN_SIZE_BYTES, timeout=TIMEOUT_SECONDS)

    # 3) Transformación
    logger.info("Transformando datos...")
    df = transform_transactions(
        input_path=RAW_CSV_PATH,
        output_parquet=CLEAN_OUTPUT_PATH,  # puedes poner None si no quieres guardar parquet
        chunksize=None  # o por ej. 100_000 si el CSV es muy grande
    )

    logger.info("ETL finalizado correctamente ✅")
    logger.info("Filas procesadas: %s", len(df))


if __name__ == "__main__":
    # Crear directorios base si no existen
    Path("data/clean").mkdir(parents=True, exist_ok=True)
    main()
