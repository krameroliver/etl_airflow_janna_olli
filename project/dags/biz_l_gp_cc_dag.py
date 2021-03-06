from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.subdag_operator import  SubDagOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import timedelta, datetime
from airflow.sensors.external_task import ExternalTaskSensor

from biz_beladung.l_gp_cc import Link_GP_CC

default_args={
        "owner":"airflow",
        'start_date': datetime(2021,6,12)
    }

link = Link_GP_CC('2018-12-31')
def join(**kwargs):
    data=link.join()
    return data

def mapping(**kwargs):
    ti=kwargs['ti']
    data = ti.xcom_pull(key='return_value')
    map=link.mapping(data=data)
    ti.xcom_push(key='link_map_data', value=map)

def load(**kwargs):
    ti=kwargs['ti']
    data_to_db = ti.xcom_pull(key='link_map_data')
    print(data_to_db)
    link.writeToDB(data_to_db)


def load_subdag(parent_dag_name, child_dag_name, args):
    d = DAG(dag_id='{0}.{1}'.format(parent_dag_name, child_dag_name),
            schedule_interval="@daily",
            default_args=args,
            catchup=False)

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



    link_gp_cc_join = PythonOperator(
        task_id="link_gp_cc_join",
        python_callable=join,
        provide_context=True,
        #op_kwargs={},
        dag=d
    )

    link_gp_cc_map = PythonOperator(
        task_id="link_gp_cc_map",
        python_callable=mapping,
        provide_context=True,
        #op_kwargs={},
        dag=d
    )

    link_gp_cc_load = PythonOperator(
       task_id="link_gp_cc_load",
       python_callable=load,
       provide_context=True,
       #op_kwargs={},
       dag=d
    )


    startAllTasks >> link_gp_cc_join >> link_gp_cc_map >> link_gp_cc_load >> endTasks
    return d
