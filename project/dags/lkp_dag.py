from airflow import DAG
from airflow.lineage import LineageBackend
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

from airflow.sensors.external_task import ExternalTaskSensor

from biz_beladung.lookup import LookUps


lkp = LookUps()
def load_lookups(**kwargs):
    lkp.insert_into_db



default_args={
        "owner":"airflow",
        'start_date': datetime(2021,6,12),
        'concurrency':1
    }

d = DAG(dag_id='load_lookups',
        schedule_interval="@daily",
        default_args=default_args,
        catchup=False)

src_dependency = ExternalTaskSensor(
    task_id='src_dag_completed_status',
    external_dag_id='load_all_source',
    external_task_id='end',  # wait for whole DAG to complete
    check_existence=True,
    start_date=datetime(2021, 6, 12))

startAllTasks = BashOperator(
    task_id='start',
    bash_command='echo "Start All Tasks"',
    dag=d
)

endTasks = BashOperator(
    task_id='end',
    bash_command='echo "All Tasks Finished"',
    dag=d
)

load_lookups = PythonOperator(
    task_id="load_lookups",
    python_callable=load_lookups,
    provide_context=True,
    dag=d
)

src_dependency >> startAllTasks >> load_lookups >> endTasks