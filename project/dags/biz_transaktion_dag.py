from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from biz_beladung.transaktion import Transaktion

default_args = {
    "owner": "airflow",
    'start_date': datetime(2021, 6, 12)
}
transaktion = Transaktion("2018-12-31")


def join(**kwargs):
    data = transaktion.join()
    return data


def mapping(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(key='return_value')
    map = transaktion.mapping(data=data)
    ti.xcom_push(key='kreditkarte_map_data', value=map)


def load(**kwargs):
    ti = kwargs['ti']
    data_to_db = ti.xcom_pull(key='kreditkarte_map_data')
    transaktion.writeToDB(data_to_db)


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

    transaktion_join = PythonOperator(
        task_id="transaktion_join",
        python_callable=join,
        provide_context=True,
        # op_kwargs={},
        dag=d
    )

    transaktion_map = PythonOperator(
        task_id="transaktion_map",
        python_callable=mapping,
        provide_context=True,
        # op_kwargs={},
        dag=d
    )

    transaktion_load = PythonOperator(
        task_id="transaktionload",
        python_callable=load,
        provide_context=True,
        # op_kwargs={},
        dag=d
    )

    startAllTasks >> transaktion_join >> transaktion_map >> transaktion_load >> endTasks
    return d
