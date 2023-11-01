import numpy
import pytest
import requests
import pendulum
import pandas as pd
from airflow import DAG
from datetime import datetime
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="python_package", schedule=None, start_date=datetime(2023, 1, 1), is_paused_upon_creation=False, catchup=False
) as dag:
  
    t_start = EmptyOperator(
        task_id='Start',
        doc_md="""Dummy Start Task"""
    )

    t_start
