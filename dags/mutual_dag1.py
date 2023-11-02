import time
from airflow import DAG
from datetime import datetime
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator, S3DeleteObjectsOperator

def raw_to_prep_and_prep_to_red():
    time.sleep(60)
    return True

with DAG(
    dag_id="mutual_dag1", schedule=None, start_date=datetime(2023, 11, 2), is_paused_upon_creation=False, catchup=False
) as dag:

    start_task = DummyOperator(task_id='start', dag=dag)

    trigger_file_sensor_task = S3KeySensor(
        task_id='trigger_file_sensor',
        bucket_name=None,
        bucket_key=[
            's3://aws-cloudtrail-logs-493179717493-0fc50be1/temp/astro-demos-sample-data/trigger_file.txt'
        ],
        aws_conn_id='aws_conn',
        poke_interval=30,
        timeout=300
    )

    mutual_exclusion_file_sensor_task = S3KeySensor(
        task_id='mutual_exclusion_file_sensor',
        bucket_name=None,
        bucket_key=[
            's3://aws-cloudtrail-logs-493179717493-0fc50be1/temp/astro-demos-sample-data/mutual_exclusion_file.txt'
        ],
        aws_conn_id='aws_conn',
        poke_interval=30,
        timeout=300
    )

    delete_mutual_exclusion_file_task = S3DeleteObjectsOperator(
      task_id='delete_mutual_exclusion_file',
      bucket='aws-cloudtrail-logs-493179717493-0fc50be1',
      keys=[
        'temp/astro-demos-sample-data/mutual_exclusion_file.txt'
      ],
      aws_conn_id='aws_conn'
    )

    raw_to_prep_and_prep_to_red_task = PythonOperator(
        task_id='raw_to_prep_and_prep_to_red_task',
        python_callable=raw_to_prep_and_prep_to_red,
    )

    create_mutual_exclusion_file_task = S3CreateObjectOperator(
        task_id="create_mutual_exclusion_file",
        aws_conn_id= 'aws_conn',
        s3_bucket='aws-cloudtrail-logs-493179717493-0fc50be1',
        s3_key='temp/astro-demos-sample-data/mutual_exclusion_file.txt',
        data=""
    )

    end_task = DummyOperator(task_id='end', dag=dag)
    
    start_task >> trigger_file_sensor_task >> mutual_exclusion_file_sensor_task >> delete_mutual_exclusion_file_task >> raw_to_prep_and_prep_to_red_task >> create_mutual_exclusion_file_task >> end_task
