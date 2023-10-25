from airflow import DAG
from datetime import datetime
from airflow.models.dag import DagModel
from airflow.models.dagrun import DagRun
from airflow.models.dagbag import DagBag
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator

def update_dag_state():
    dag_bag = DagBag(read_dags_from_db=False)
    for dag_id_ in dag_bag.dag_ids:
        print("dag_id =>", dag_id_)
        
    dag_run = DagRun.query(func.max(execution_date).label('execution_date')).group_by(dag_id)
    print(dag_run)
    dag_runs = DagRun.find(dag_id="mutual_exclusion_dag")
    #print(dag_runs)
    dag_model = DagModel.get_dagmodel("DAG1")
    dag_model.set_is_paused(True)

with DAG(
    dag_id="mutual_exclusion_dag", schedule=None, start_date=datetime(2023, 10, 25), is_paused_upon_creation=False, catchup=False
) as dag:

    start_task = DummyOperator(task_id='start_task', dag=dag)
    
    task_1 = PythonOperator(
        task_id = 'task_1',
        python_callable = update_dag_state,
        # op_kwargs={
        #     "dag_list": ["DAG1","DAG2"]
        # }
    )
    
    start_task >> task_1
