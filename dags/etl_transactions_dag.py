from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from datetime import datetime, timedelta
import os
import logging

# Configuración del DAG
default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}

# Definir rutas relativas dentro del contenedor
CSV_PATH = "/opt/airflow/project/data/sample_transactions.csv"
SQLITE_PATH = "/opt/airflow/project/data/transactions.db"

# Importar funciones
from etl.transform import transform_transactions
from etl.load import load_dataframe_to_sqlite

# Configurar logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

with DAG(
    dag_id="etl_transactions_dag",
    description="ETL local de transacciones (transform + carga)",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["etl", "sqlite"]
) as dag:

    def task_transformar():
        logger.info(f"✅ Iniciando transformación del archivo: {CSV_PATH}")
        df = transform_transactions(CSV_PATH)
        if df.empty:
            logger.warning("⚠️ DataFrame resultante está vacío. Abortando DAG.")
            return False
        logger.info(f"✅ Transformación completada. Total filas: {len(df)}")
        return True

    def task_cargar():
        logger.info(f"🚀 Cargando datos a base SQLite: {SQLITE_PATH}")
        df = transform_transactions(CSV_PATH)
        load_dataframe_to_sqlite(df, db_path=SQLITE_PATH, table_name="transactions")
        logger.info("✅ Carga completada en SQLite")

    transformar = ShortCircuitOperator(
        task_id="transformar_datos",
        python_callable=task_transformar
    )

    cargar = PythonOperator(
        task_id="cargar_sqlite",
        python_callable=task_cargar
    )

    transformar >> cargar
