import os
from airflow import DAG
from time import time_ns
from datetime import datetime
from airflow.models.connection import Connection
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python_operator import PythonOperator

def func():
    print("DAG1 is runningg")

with DAG(
    dag_id="DAG1", schedule=None, start_date=datetime(2023, 1, 1), is_paused_upon_creation=False, catchup=False
) as dag:

    task_1 = PythonOperator(
            task_id='DAG1',
            python_callable=func
        )
  
    trigger_dependent_dag = TriggerDagRunOperator(
        task_id="trigger_dependent_dag",
        trigger_dag_id="DAG2",
        wait_for_completion=True
    )
		
    end_task = DummyOperator(task_id='end_task', dag=dag)

    task_1 >> trigger_dependent_dag >> end_task
