
from datetime import timedelta,datetime,date
from pathlib import Path
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import requests
import pandas as pd
import psycopg2
import os 

from psycopg2.extras import execute_values


from  ExtracAPI import Extraer_data, Filtrar_data, conexion_tabla, cargar_en_postgres 
dag_path = os.getcwd()
default_args = {
    start_date: datetime(2024, 4, 2),
    retries: 1,
    retry_delay: timedelta(minutes=5)
}

#nombre del dag
ingestion_dag = DAG(
    dag_id=ingestion_data,
    default_args=default_args,
    description=Agregar datos de las top 15 cryptomonedas en una tabla de redshift,
     schedule_interval=timedelta(days=1),
    catchup=False
)


task_1 = PythonOperator(
    task_id=Extraerdata,
    python_callable=Extraer_data,
    #op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=ingestion_dag,
)

task_2 = PythonOperator(
    task_id=Filtrardatos,
    python_callable=Filtrar_data,
    dag=ingestion_dag,
)

task_3 = PythonOperator(
    task_id=conexionconredshift,
    python_callable=conexion_tabla,
    dag=ingestion_dag,
)

task_4 = PythonOperator(
    task_id=CargaenDB,
    python_callable=cargar_en_postgres,
    dag=ingestion_dag,
)



task_1 >> task_2 >> task_3 >> task_4