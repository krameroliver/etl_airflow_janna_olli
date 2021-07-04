from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.subdag_operator import  SubDagOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import timedelta, datetime

#try:
from airflow.sensors.external_task import ExternalTaskSensor

from biz_beladung.darlehen import Darlehen
#except ImportError:
#    from project.dags.biz_beladung.geschaeftspartner import Gp

default_args={
        "owner":"airflow",
        'start_date': datetime(2021,6,12)
    }


darlehen = Darlehen("2018-12-31")


def join(**kwargs):
    data=darlehen.join()
    return data
    #ti=context['task_instance']
    #ti.xcom_push(key='gp_data', value=data)

def mapping(**kwargs):
    ti=kwargs['ti']
    data = ti.xcom_pull(key='return_value')
    map=darlehen.mapping(data=data)
    ti.xcom_push(key='darlehen_map_data', value=map)

def load(**kwargs):
    ti=kwargs['ti']
    data_to_db = ti.xcom_pull(key='darlehen_map_data')
    print(data_to_db)
    darlehen.writeToDB(data_to_db)



d = DAG(dag_id='load_darlehen_biz',
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
    bash_command='echo "Start darlehen Tasks"',
    dag=d
)

endTasks = BashOperator(
    task_id='end',
    bash_command='echo "darlehen Tasks Finished"',
    dag=d
)



darlehen_join = PythonOperator(
    task_id="darlehen_join",
    python_callable=join,
    provide_context=True,
    #op_kwargs={},
    dag=d
)

darlehen_map = PythonOperator(
    task_id="darlehen_map",
    python_callable=mapping,
    provide_context=True,
    #op_kwargs={},
    dag=d
)

darlehen_load = PythonOperator(
   task_id="darlehen_load",
   python_callable=load,
   provide_context=True,
   #op_kwargs={},
   dag=d
)


src_dependency >> startAllTasks >> darlehen_join >> darlehen_map >> darlehen_load >> endTasks
