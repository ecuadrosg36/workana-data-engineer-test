import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator

# -------------------------------------------------------------------
# Asegúrate de que Airflow pueda importar tus módulos del proyecto
# Ajusta esta ruta si montas tu repo en otro path dentro del contenedor
# -------------------------------------------------------------------
PROJECT_ROOT = os.getenv("PROJECT_ROOT", "/opt/airflow/project")
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

# Importar tus funciones del proyecto
from scripts.download_csv import download_csv
from etl.sensors import wait_for_file
from etl.transform import transform_transactions
from etl.load import load_dataframe_to_sqlite
from etl.config import (
    CSV_URL,
    RAW_CSV_PATH,
    MIN_SIZE_BYTES,
    TIMEOUT_SECONDS,
)

# -------------------------------------------------------------------
# Configuración general del DAG
# -------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "depends_on_past": False,
}

with DAG(
    dag_id="etl_transactions_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,          # Ejecutarlo manualmente
    catchup=False,
    default_args=default_args,
    description="Pipeline ETL local (CSV -> SQLite) con sensores y reintentos",
    tags=["etl", "sqlite", "workana"],
) as dag:

    # ---------------------
    # Python callables
    # ---------------------
    def task_descargar():
        logger.info("🔁 Descarga omitida temporalmente (el archivo ya está presente)")
        return True



    def task_esperar_archivo():
        return wait_for_file(
            str(RAW_CSV_PATH),
            min_size_bytes=MIN_SIZE_BYTES,
            timeout=TIMEOUT_SECONDS
        )

    def task_transformar():
        df = transform_transactions(str(RAW_CSV_PATH))
        # ShortCircuitOperator continúa solo si True
        return len(df) > 0

    def task_cargar():
        df = transform_transactions(str(RAW_CSV_PATH))
        load_dataframe_to_sqlite(df, table_name="transactions")

    # ---------------------
    # Tareas
    # ---------------------
    descargar = PythonOperator(
        task_id="descargar_csv",
        python_callable=task_descargar,
        retries=3,
        retry_delay=timedelta(seconds=30),
        execution_timeout=timedelta(minutes=2),
    )

    esperar = ShortCircuitOperator(
        task_id="esperar_archivo",
        python_callable=task_esperar_archivo,
        retries=3,
        retry_delay=timedelta(seconds=20),
        execution_timeout=timedelta(minutes=2),
    )

    transformar = ShortCircuitOperator(
        task_id="transformar_datos",
        python_callable=task_transformar,
        retries=2,
        retry_delay=timedelta(seconds=30),
        execution_timeout=timedelta(minutes=3),
    )

    cargar = PythonOperator(
        task_id="cargar_a_sqlite",
        python_callable=task_cargar,
        retries=2,
        retry_delay=timedelta(seconds=30),
        execution_timeout=timedelta(minutes=5),
    )

    # ---------------------
    # Flujo
    # ---------------------
    descargar >> esperar >> transformar >> cargar
