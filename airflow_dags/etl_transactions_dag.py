from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from datetime import datetime, timedelta

from scripts.download_csv import download_csv
from etl.sensors import wait_for_file
from etl.transform import transform_transactions
from etl.load import load_dataframe_to_sqlite

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "depends_on_past": False,
}

with DAG(
    dag_id="etl_transactions_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    description="Pipeline ETL local para transacciones CSV con SQLite",
    tags=["etl", "sqlite", "workana"]
) as dag:

    def task_descargar():
        from etl.config import CSV_URL, RAW_CSV_PATH
        download_csv(CSV_URL, str(RAW_CSV_PATH))

    def task_esperar_archivo():
        from etl.config import RAW_CSV_PATH, MIN_SIZE_BYTES, TIMEOUT_SECONDS
        return wait_for_file(str(RAW_CSV_PATH), min_size_bytes=MIN_SIZE_BYTES, timeout=TIMEOUT_SECONDS)

    def task_transformar():
        from etl.config import RAW_CSV_PATH
        df = transform_transactions(str(RAW_CSV_PATH))
        return len(df) > 0

    def task_cargar():
        from etl.config import RAW_CSV_PATH
        df = transform_transactions(str(RAW_CSV_PATH))
        load_dataframe_to_sqlite(df, table_name="transactions")

    descargar = PythonOperator(
        task_id="descargar_csv",
        python_callable=task_descargar
    )

    esperar = ShortCircuitOperator(
        task_id="esperar_archivo",
        python_callable=task_esperar_archivo
    )

    transformar = ShortCircuitOperator(
        task_id="transformar_datos",
        python_callable=task_transformar
    )

    cargar = PythonOperator(
        task_id="cargar_a_sqlite",
        python_callable=task_cargar
    )

    descargar >> esperar >> transformar >> cargar
