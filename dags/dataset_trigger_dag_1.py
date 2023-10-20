from airflow import DAG
from airflow.datasets import Dataset
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator

dataset_1 = Dataset("s3://dataset-bucket/example.csv")

with DAG(
    dag_id="dataset_trigger_dag_1", schedule=None, start_date=datetime(2023, 1, 1), is_paused_upon_creation=False, catchup=False
) as dag:
    
    t_start = EmptyOperator(
        task_id='Start',
        doc_md="""Dummy Start Task""",
        outlets=[dataset_1]
    )

  t_start
