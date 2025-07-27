from pathlib import Path

# --- Rutas principales ---
DATA_DIR = Path("data")
RAW_CSV_PATH = DATA_DIR / "sample_transactions.csv"
CLEAN_OUTPUT_PATH = DATA_DIR / "clean" / "sample_transactions_clean.parquet"

# --- Sensor ---
MIN_SIZE_BYTES = 1_000
TIMEOUT_SECONDS = 120

# --- URL de Dropbox ---
CSV_URL = "https://www.dropbox.com/scl/fi/9s8zptquw3urvhsctmgya/sample_transactions.csv?rlkey=8s2wyvvjcm96c6ctopvwnskwj&dl=1"

# --- Configuración de SQLite ---
SQLITE_DB_PATH = DATA_DIR / "etl_output.db"
SQLITE_URL = f"sqlite:///{SQLITE_DB_PATH}"
