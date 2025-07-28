# dags/dag_alertas_etl.py
import sys
import os
sys.path.insert(0, os.environ.get("PROJECT_ROOT", "/opt/airflow/project"))


import sqlite3
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

DB_PATH = "/opt/airflow/project/data/transactions.db"
logger = logging.getLogger("alertas_etl")

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(seconds=20),
}

def detectar_anomalias():
    logger.info("📊 Ejecutando query de anomalías de volumen")
    conn = sqlite3.connect(DB_PATH)
    query = "SELECT * FROM anomalies_by_volume ORDER BY ABS(pct_diff) DESC LIMIT 5"
    result = conn.execute(query).fetchall()
    conn.close()

    if result:
        logger.warning(f"🚨 Anomalías detectadas: {len(result)} registros")
        for r in result:
            logger.warning(f"Fecha: {r[0]}, Volumen: {r[1]}, Desviación: {r[3]:.2f}%")
    else:
        logger.info("✅ Sin anomalías de volumen detectadas.")

def detectar_usuarios_fallidos():
    logger.info("🔍 Ejecutando query de fallos frecuentes")
    conn = sqlite3.connect(DB_PATH)
    query = "SELECT * FROM frequent_failures_last7days"
    result = conn.execute(query).fetchall()
    conn.close()

    if result:
        logger.warning(f"🚨 Usuarios con fallos frecuentes: {len(result)} casos")
        for r in result:
            logger.warning(f"Usuario: {r[0]}, Fallos: {r[1]}")
    else:
        logger.info("✅ Sin usuarios con fallos frecuentes.")

with DAG(
    dag_id="alertas_etl",
    description="Simulación de alertas basadas en vistas SQL",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["alertas", "sql"],
) as dag:

    alerta_anomalias = PythonOperator(
        task_id="alerta_anomalias_volumen",
        python_callable=detectar_anomalias,
    )

    alerta_usuarios = PythonOperator(
        task_id="alerta_usuarios_fallidos",
        python_callable=detectar_usuarios_fallidos,
    )

    alerta_anomalias >> alerta_usuarios
