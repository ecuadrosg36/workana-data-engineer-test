import requests
import os

def download_csv(url: str, output_path: str):
    print(f"🔽 Descargando desde: {url}")
    response = requests.get(url, stream=True)

    # Verifica código de estado
    if response.status_code != 200:
        raise Exception(f"❌ Error al descargar archivo. Código HTTP: {response.status_code}")

    # Verifica tipo de contenido
    content_type = response.headers.get("Content-Type", "")
    if "text/html" in content_type:
        raise Exception("❌ Error: El archivo descargado parece ser HTML (¿URL sin 'dl=1'?)")

    # Guarda el archivo si todo está bien
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    file_size = os.path.getsize(output_path)
    print(f"✅ Archivo guardado correctamente en {output_path} ({file_size:,} bytes)")


# Uso manual (debug local)
if __name__ == "__main__":
    url = "https://www.dropbox.com/scl/fi/9s8zptquw3urvhsctmgya/sample_transactions.csv?rlkey=8s2wyvvjcm96c6ctopvwnskwj&dl=1"
    output_path = "data/sample_transactions.csv"
    download_csv(url, output_path)
