import requests
import os

def download_csv(url: str, output_path: str):
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"✅ Archivo descargado correctamente en: {output_path}")
    else:
        print(f"❌ Error al descargar archivo. Código de estado: {response.status_code}")

if __name__ == "__main__":
    url = "https://www.dropbox.com/scl/fi/9s8zptquw3urvhsctmgya/sample_transactions.csv?rlkey=8s2wyvvjcm96c6ctopvwnskwj&dl=1"
    output_path = "data/sample_transactions.csv"
    download_csv(url, output_path)
