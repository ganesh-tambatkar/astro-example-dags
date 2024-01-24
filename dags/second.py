import logging
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from datetime import datetime, timedelta, date, timezone
from airflow.utils.session import provide_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
args = {"owner": "airflow", "start_date": airflow.utils.dates.days_ago(1)}

dag = DAG(
 dag_id="second", default_args=args, schedule_interval="32 07 * * *"
)
def pp():
 print("Second Dependent Task")
 print(timedelta(minutes=10))

def get_date():
  @provide_session
  def execution_date_fn(session=None,  **kwargs):
   return datetime.now(timezone.utc)
  return execution_date_fn
 
with dag:
 Second_Task=PythonOperator(task_id="Second_Task", python_callable=pp,dag=dag)
ExternalTaskSensor(
 task_id="Ext_Sensor_Task",
 external_dag_id="first",
 external_task_id="first_task",
 # execution_delta = datetime.now(),
 execution_date_fn = get_date(),
 timeout=300,
 dag=dag)>>Second_Task
