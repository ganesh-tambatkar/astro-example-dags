from airflow import DAG
from datetime import datetime, timedelta
from airflow.models.dag import DagModel
from airflow.models.dagrun import DagRun
from airflow.models.dagbag import DagBag
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator

def update_dag_state(dag_list):
    
    # dag_bag = DagBag(read_dags_from_db=False)
    # for dag_id_ in dag_bag.dag_ids:
    #     print("dag_id =>", dag_id_)

    active_run_dag_id = None
    active_dag_run = DagRun.active_runs_of_dags(dag_ids = dag_list)
    for dagId in dag_list:
        try:
            active_dag_run_count = active_dag_run[dagId]
            print("active_dag_run_count =>", str(active_dag_run_count))
        except:
            active_dag_run_count = 0
        if active_dag_run_count >= 1:
            active_run_dag_id = dagId
            break

    print("active_run_dag_id =>", active_run_dag_id)
    try:
        dag_list.remove(active_run_dag_id)
    except:
        pass
    print(dag_list)
    
    if active_run_dag_id is not None:
        for dagId in dag_list:
            dag_model = DagModel.get_dagmodel(dagId)
            dag_model.set_is_paused(True)
    else:
        for dagId in dag_list:
            dag_model = DagModel.get_dagmodel(dagId)
            dag_model.set_is_paused(False)
    
    # dag_runs = DagRun.find(dag_id="mutual_exclusion_dag")
    # #print(dag_runs)

with DAG(
    dag_id="mutual_exclusion_dag", schedule=timedelta(seconds=30), start_date=datetime(2023, 10, 25), is_paused_upon_creation=False, catchup=False
) as dag:

    start_task = DummyOperator(task_id='start_task', dag=dag)
    
    task_1 = PythonOperator(
        task_id = 'task_1',
        python_callable = update_dag_state,
        op_kwargs={
            "dag_list": ["s3_transform","DAG2","external_task_sensor_dag"]
        }
    )
    
    start_task >> task_1
