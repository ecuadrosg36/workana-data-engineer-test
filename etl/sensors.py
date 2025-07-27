import os
import time

def wait_for_file(path: str, min_size_bytes: int = 1000, timeout: int = 60):
    waited = 0
    while waited < timeout:
        if os.path.exists(path) and os.path.getsize(path) >= min_size_bytes:
            print(f"✅ Archivo disponible: {path}")
            return True
        print(f"⏳ Esperando archivo... ({waited}s)")
        time.sleep(5)
        waited += 5
    raise TimeoutError(f"❌ No se encontró el archivo o no alcanzó el tamaño mínimo en {timeout} segundos.")

if __name__ == "__main__":
    wait_for_file("data/sample_transactions.csv", min_size_bytes=1000)
