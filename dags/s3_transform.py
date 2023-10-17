from airflow.providers.amazon.aws.operators.s3 import S3FileTransformOperator
from airflow import DAG
from airflow.models.connection import Connection
from time import time_ns
from datetime import datetime
import os
from airflow.operators.python_operator import PythonOperator

conn = Connection(
    conn_id="aws_demo",
    conn_type="aws",
    extra={
        "config_kwargs": {
            "signature_version": "unsigned",
        },
    },
)

env_key = f"AIRFLOW_CONN_{conn.conn_id.upper()}"
conn_uri = conn.get_uri()
os.environ[env_key] = conn_uri

def print_conn():
    print("conn =>", conn)
    print("conn_uri =>", conn_uri)
    return conn, conn_uri

with DAG(
    dag_id="s3", schedule="@once", start_date=datetime(2023, 1, 1), is_paused_upon_creation=False, catchup=False
) as dag:

    s3_file = S3ListOperator(
        task_id='list_3s_files',
        bucket='aws-cloudtrail-logs-493179717493-0fc50be1',
        prefix='AWSLogs/493179717493/CloudTrail-Digest/af-south-1/2023/06/02/',
        delimiter='/',
        aws_conn_id='aws_conn'
    )

    task_1 = PythonOperator(
            task_id='print_conn',
            python_callable=print_conn,
            doc_md=""" print_conn """
        )
    
    task_2 = S3FileTransformOperator(
        task_id="s3transform",
        source_s3_key="s3://astro-demos-sample-data/countries.csv",
        source_aws_conn_id=conn.conn_id,
        transform_script="/usr/local/airflow/transform_script.sh", # select_expression doesn't work with anonymous S3 access, so have to use transform_script instead. this script was injected by the Dockerfile
        dest_aws_conn_id=conn.conn_id,
        dest_s3_key=f"s3://astro-demos-sample-data/uploads/{time_ns()}/europian_countries.csv",
    )

    s3_file >> task_1 >> task_2
