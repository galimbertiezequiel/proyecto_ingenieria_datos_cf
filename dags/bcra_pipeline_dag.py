
#Importar librerias
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Parámetros del DAG
default_args = {
    'owner': 'ezequiel',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'bcra_data_pipeline',
    default_args=default_args,
    description='Pipeline para extraer y transformar datos del BCRA',
    schedule="0 13 * * *",  # 13:00 UTC = 10:00 AM Argentina
    catchup=False,
)

# Tarea 1: Ejecutar el script Python de extracción y carga
extract_task = BashOperator(
    task_id='extract_bcra_data',
    bash_command='python C:/Users/galim/Desktop/airflow_proyecto/scryp_py/airflow_proyecto_scryp.py',
    dag=dag,
)

# Tarea 2: Ejecutar dbt run para transformar los datos
dbt_run_task = BashOperator(
    task_id='run_dbt',
    bash_command='cd C:/Users/galim/Desktop/airflow_proyecto/bcra_dbt && dbt run',
    dag=dag,
)

# Definir el orden de ejecución: primero extracción, luego dbt
extract_task >> dbt_run_task
