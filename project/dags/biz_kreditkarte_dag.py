from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from biz_beladung.kreditkarte import Kreditkarte

default_args = {
    "owner": "airflow",
    'start_date': datetime(2021, 6, 12)
}

kreditkarte = Kreditkarte("2018-12-31")


def join(**kwargs):
    data = kreditkarte.join()
    return data


def mapping(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(key='return_value')
    map = kreditkarte.mapping(data=data)
    ti.xcom_push(key='kreditkarte_map_data', value=map)


def load(**kwargs):
    ti = kwargs['ti']
    data_to_db = ti.xcom_pull(key='kreditkarte_map_data')
    kreditkarte.writeToDB(data_to_db)


def load_subdag(parent_dag_name, child_dag_name, args):
    d = DAG(dag_id='{0}.{1}'.format(parent_dag_name, child_dag_name),
            schedule_interval="@daily",
            default_args=args,
            catchup=False)

    startAllTasks = BashOperator(
        task_id='start',
        bash_command='echo "Start kreditkarte Tasks"',
        dag=d
    )

    endTasks = BashOperator(
        task_id='end',
        bash_command='echo "kreditkarte Tasks Finished"',
        dag=d
    )

    kreditkarte_join = PythonOperator(
        task_id="kreditkarte_join",
        python_callable=join,
        provide_context=True,
        # op_kwargs={},
        dag=d
    )

    kreditkarte_map = PythonOperator(
        task_id="kreditkarte_map",
        python_callable=mapping,
        provide_context=True,
        # op_kwargs={},
        dag=d
    )

    kreditkarte_load = PythonOperator(
        task_id="kreditkarte_load",
        python_callable=load,
        provide_context=True,
        # op_kwargs={},
        dag=d
    )

    startAllTasks >> kreditkarte_join >> kreditkarte_map >> kreditkarte_load >> endTasks
    return d
