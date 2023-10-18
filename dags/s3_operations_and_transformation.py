import os
from airflow import DAG
from time import time_ns
from airflow.operators.python_operator import PythonOperator

with DAG(
    dag_id="s3_operations_and_transformation", schedule="@once", start_date=datetime(2023, 1, 1), is_paused_upon_creation=False, catchup=False
) as dag:
    
  t_start = DummyOperator(
        task_id='Start',
        doc_md="""Dummy Start Task"""
  )

  t_start

  
