import os
import pandas as pd
from airflow import DAG
from time import time_ns
from datetime import datetime
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator

def transform_file(input_file_path, ouput_file_path, **kwargs):
    hook = S3Hook("aws_conn")
    temp_input_file_path = hook.download_file(key = input_file_path)
    input_df = pd.read_csv(temp_input_file_path)
    input_df['name'] = input_df['name'].str.upper()
    TEMP_FILE_PATH = '/tmp/output_file.csv'
    input_df.to_csv(TEMP_FILE_PATH, sep=",", index=False)
    
    t_upload_output_file_to_s3 = LocalFilesystemToS3Operator(
        task_id="upload_output_file_to_s3",
        filename=TEMP_FILE_PATH,
        dest_key=ouput_file_path,
        # dest_bucket=s3_bucket_name,
        replace=True
    )
    
    t_upload_output_file_to_s3.execute(context=kwargs)

with DAG(
    dag_id="s3_operations_and_transformation", schedule=None, start_date=datetime(2023, 1, 1), is_paused_upon_creation=False, catchup=False
) as dag:
    
    t_start = DummyOperator(
        task_id='Start',
        doc_md="""Dummy Start Task"""
    )

    t_transform_file = PythonOperator(
        task_id='transform_file',
        python_callable=transform_file,
        op_kwargs={
            "input_file_path": "s3://aws-cloudtrail-logs-493179717493-0fc50be1/temp/astro-demos-sample-data/input_file.csv",
            "ouput_file_path":"s3://aws-cloudtrail-logs-493179717493-0fc50be1/temp/astro-demos-sample-data/input_file.csv"
        },
        provide_context=True,
        doc_md=""" transform_file function """
    )
    
    t_start >> t_transform_file

  
