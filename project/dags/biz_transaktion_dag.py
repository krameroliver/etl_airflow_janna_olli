from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.subdag_operator import  SubDagOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import timedelta, datetime

#try:
from airflow.sensors.external_task import ExternalTaskSensor

from biz_beladung.transaktion import Transaktion
#except ImportError:
#    from project.dags.biz_beladung.geschaeftspartner import Gp

default_args={
        "owner":"airflow",
        'start_date': datetime(2021,6,12)
    }
transaktion = Transaktion("2018-12-31")
def join(**kwargs):
    data=transaktion.join()
    return data

def mapping(**kwargs):
    ti=kwargs['ti']
    data = ti.xcom_pull(key='return_value')
    map=transaktion.mapping(data=data)
    ti.xcom_push(key='kreditkarte_map_data', value=map)

def load(**kwargs):
    ti=kwargs['ti']
    data_to_db = ti.xcom_pull(key='kreditkarte_map_data')
    print(data_to_db)
    transaktion.writeToDB(data_to_db)



d = DAG(dag_id='load_transaktion_biz',
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



transaktion_join = PythonOperator(
    task_id="transaktion_join",
    python_callable=join,
    provide_context=True,
    #op_kwargs={},
    dag=d
)

transaktion_map = PythonOperator(
    task_id="transaktion_map",
    python_callable=mapping,
    provide_context=True,
    #op_kwargs={},
    dag=d
)

transaktion_load = PythonOperator(
   task_id="transaktionload",
   python_callable=load,
   provide_context=True,
   #op_kwargs={},
   dag=d
)


src_dependency >> startAllTasks >> transaktion_join >> transaktion_map >> transaktion_load >> endTasks
#startAllTasks >> load_db_acct >> endTasks