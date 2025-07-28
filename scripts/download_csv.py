import requests
import os

def download_csv(url: str, output_path: str):
    print("🔽 Iniciando descarga del archivo CSV...")
    print(f"🔗 URL: {url}")
    print(f"📥 Guardando en: {output_path}")

    headers = {
        "User-Agent": "Mozilla/5.0"  # Soluciona problemas con Dropbox
    }

    response = requests.get(url, stream=True, headers=headers)

    if response.status_code != 200:
        raise Exception(f"❌ Error al descargar archivo. Código HTTP: {response.status_code}")

    content_type = response.headers.get("Content-Type", "")
    if "text/html" in content_type:
        raise Exception("❌ Error: El archivo descargado parece ser HTML (¿URL sin 'dl=1'?)")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    size = os.path.getsize(output_path)
    print(f"✅ Archivo descargado correctamente en {output_path} ({size:,} bytes)")
