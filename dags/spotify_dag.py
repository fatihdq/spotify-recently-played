from datetime import datetime,timedelta
import pymongo
from data_collection import *
from etl import *

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd

dag_id = "SPOTIFY"

default_args = {
    'owner': 'fatih',
    'depends_on_past': False,
    'email': ['fatih.dq23@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(dag_id=dag_id, default_args=default_args, start_date= datetime(2021,9,20),
         schedule_interval='@daily',catchup=False) as dag:

    collect = PythonOperator(
        task_id='Collect_data',
        python_callable = data_collection
    )

    etl = PythonOperator(
        task_id = 'extract_transform_load',
        python_callable = extract_transform_load
    )

    collect >> etl
