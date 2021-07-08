from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from biz_beladung.l_cc_konto import Link_CC_Konto

default_args = {
    "owner": "airflow",
    'start_date': datetime(2021, 6, 12)
}

link = Link_CC_Konto('2018-12-31')


def join(**kwargs):
    data = link.join()
    return data


def mapping(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(key='return_value')
    map = link.mapping(data=data)
    ti.xcom_push(key='link_map_data', value=map)


def load(**kwargs):
    ti = kwargs['ti']
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

    link_cc_konto_join = PythonOperator(
        task_id="link_cc_konto_join",
        python_callable=join,
        provide_context=True,
        # op_kwargs={},
        dag=d
    )

    link_cc_konto_map = PythonOperator(
        task_id="link_cc_konto_map",
        python_callable=mapping,
        provide_context=True,
        # op_kwargs={},
        dag=d
    )

    link_cc_konto_load = PythonOperator(
        task_id="link_cc_konto_load",
        python_callable=load,
        provide_context=True,
        # op_kwargs={},
        dag=d
    )

    startAllTasks >> link_cc_konto_join >> link_cc_konto_map >> link_cc_konto_load >> endTasks
    return d
