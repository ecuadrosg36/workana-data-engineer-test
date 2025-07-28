# etl/sensors.py
import os
import time
import logging

logger = logging.getLogger(__name__)

def file_ready(path: str, min_size_bytes: int = 1_000) -> bool:
    """
    Función simple (booleana) para usar con PythonSensor.
    Devuelve True cuando el archivo existe y supera el tamaño mínimo.
    """
    if not os.path.exists(path):
        return False
    size = os.path.getsize(path)
    return size >= min_size_bytes

def wait_for_file(path: str, min_size_bytes: int = 1_000, timeout: int = 60) -> bool:
    """
    Versión blocking/polling para uso standalone (no usada por el DAG).
    """
    logger.info("Esperando archivo %s (min_size_bytes=%s)", path, min_size_bytes)
    waited = 0
    while waited < timeout:
        if file_ready(path, min_size_bytes=min_size_bytes):
            logger.info("✅ Archivo disponible: %s (%s bytes)", path, os.path.getsize(path))
            return True
        logger.info("⏳ Esperando archivo... (%ss)", waited)
        time.sleep(5)
        waited += 5
    raise TimeoutError(
        f"❌ No se encontró el archivo o no alcanzó el tamaño mínimo en {timeout} segundos."
    )
