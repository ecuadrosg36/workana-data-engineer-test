from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def dummy_task():
    print("Running sample task")

with DAG(dag_id='workana_sample_dag',
         start_date=datetime(2023, 1, 1),
         schedule_interval=None,
         catchup=False) as dag:

    run = PythonOperator(
        task_id='run_sample',
        python_callable=dummy_task
    )
