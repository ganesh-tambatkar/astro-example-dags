from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskMarker, ExternalTaskSensor

with DAG(
    dag_id="external_task_sensor_dag", schedule=None, start_date=datetime(2023, 1, 1), is_paused_upon_creation=False, catchup=False
) as dag:
    
    t_start = EmptyOperator(
        task_id='Start',
        doc_md="""Dummy Start Task"""
    )

    t_external_task_sensor = ExternalTaskSensor(
        task_id="parent_task_sensor",
        external_task_id="trigger_dependent_dag",
        external_dag_id="DAG1",
        execution_delta = timedelta(minutes=10),
        timeout=300,
        dag=dag
    )

    t_end = EmptyOperator(
        task_id='End',
        doc_md="""Dummy End Task"""
    )

    t_start >> t_external_task_sensor >> t_end

    
