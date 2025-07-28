# dags/etl_transactions_dag.py
import sys
import os
sys.path.insert(0, os.environ.get("PROJECT_ROOT", "/opt/airflow/project"))

import time
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator, get_current_context
from airflow.sensors.python import PythonSensor

# Rutas
CSV_PATH = "/opt/airflow/project/data/sample_transactions.csv"
SQLITE_PATH = "/opt/airflow/project/data/transactions.db"

# Importaciones del proyecto
from etl.transform import transform_transactions
from etl.load import load_dataframe_to_sqlite, validate_table_not_empty
from etl.sensors import file_ready
from etl.utils.metrics import log_metric

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="etl_transactions_dag",
    description="ETL local de transacciones (sensor + transform + load + métricas)",
    start_date=datetime(2024, 1, 1),
    schedule=None,   # reemplaza schedule_interval (deprecated)
    catchup=False,
    default_args=default_args,
    tags=["etl", "sqlite", "metrics"]
) as dag:

    # --- SENSOR: espera archivo con tamaño mínimo ---
    esperar_archivo = PythonSensor(
        task_id="esperar_archivo",
        python_callable=file_ready,
        op_args=[CSV_PATH],
        op_kwargs={"min_size_bytes": 5_000},  # 5KB mínimo
        poke_interval=5,
        timeout=60,  # segundos
        mode="poke",  # 'reschedule' si quieres liberar el worker entre pokes
        doc_md="Sensor que espera a que el CSV exista y tenga un tamaño mínimo.",
    )

    def _transformar():
        ctx = get_current_context()
        dag_run_id = ctx["dag_run"].run_id

        start = time.time()
        status = "OK"
        rows = None
        error = None

        try:
            logger.info(f"✅ Iniciando transformación del archivo: {CSV_PATH}")
            df = transform_transactions(CSV_PATH)
            rows = len(df)
            if df.empty:
                status = "EMPTY"
                logger.warning("⚠️ DataFrame resultante está vacío. Abortando DAG.")
                return False
            logger.info(f"📊 Registros transformados: {rows}")
            return True
        except Exception as e:
            status = "FAIL"
            error = str(e)
            logger.exception("❌ Error en transformar_datos")
            raise
        finally:
            duration = time.time() - start
            log_metric(
                task_name="transformar_datos",
                status=status,
                duration_s=duration,
                row_count=rows,
                dag_run_id=dag_run_id,
                error=error,
            )

    transformar = ShortCircuitOperator(
        task_id="transformar_datos",
        python_callable=_transformar
    )

    def _cargar():
        ctx = get_current_context()
        dag_run_id = ctx["dag_run"].run_id

        start = time.time()
        status = "OK"
        rows = None
        error = None

        try:
            logger.info(f"🚀 Cargando datos a base SQLite: {SQLITE_PATH}")
            df = transform_transactions(CSV_PATH)
            rows = len(df)
            load_dataframe_to_sqlite(df=df, sqlite_path=SQLITE_PATH, table_name="transactions")
            logger.info("✅ Carga completada en SQLite")
        except Exception as e:
            status = "FAIL"
            error = str(e)
            logger.exception("❌ Error en cargar_sqlite")
            raise
        finally:
            duration = time.time() - start
            log_metric(
                task_name="cargar_sqlite",
                status=status,
                duration_s=duration,
                row_count=rows,
                dag_run_id=dag_run_id,
                error=error,
            )

    cargar = PythonOperator(
        task_id="cargar_sqlite",
        python_callable=_cargar
    )

    def _validar_tabla_no_vacia():
        ctx = get_current_context()
        dag_run_id = ctx["dag_run"].run_id

        start = time.time()
        status = "OK"
        count = None
        error = None

        try:
            count = validate_table_not_empty(sqlite_path=SQLITE_PATH, table_name="transactions")
            logger.info("✅ Validación OK. Filas: %s", count)
            return True
        except Exception as e:
            status = "FAIL"
            error = str(e)
            logger.exception("❌ Error validando tabla destino")
            raise
        finally:
            duration = time.time() - start
            log_metric(
                task_name="validar_tabla_not_empty",
                status=status,
                duration_s=duration,
                row_count=count,
                dag_run_id=dag_run_id,
                error=error,
            )

    validar = ShortCircuitOperator(
        task_id="validar_tabla_not_empty",
        python_callable=_validar_tabla_no_vacia
    )

    # Dependencias
    esperar_archivo >> transformar >> cargar >> validar
