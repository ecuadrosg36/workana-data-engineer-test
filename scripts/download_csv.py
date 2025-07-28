import os
import logging

def download_csv(url: str, output_path: str):
    logger = logging.getLogger(__name__)
    if os.path.exists(output_path):
        logger.info(f"📁 Archivo ya existe en: {output_path}. Simulando descarga.")
        return
    raise Exception("❌ Simulación: Archivo no existe. Por favor colócalo manualmente.")
