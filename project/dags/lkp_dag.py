from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from dwhutils.build_dynamic_lookup import dynamic_lkp
from dwhutils.build_static_lookup import static_lookup

s_lkp = static_lookup()
d_lkp = dynamic_lkp()


def load_d_lookups(tablename: str, column: str, lkp_name: str):
    d_lkp.post_lkp(tablename=tablename, column=column, lookup_name=lkp_name)


def load_s_lookups(lkp_name: str, lkp_data):
    s_lkp.build_lkp(lookup_name=lkp_name, lkp_data=lkp_data)


default_args = {
    "owner": "airflow",
    'start_date': datetime(2021, 6, 12),
    'concurrency': 1
}

d = DAG(dag_id='load_lookups',
        schedule_interval="@once",
        default_args=default_args,
        tags=['ENB'],
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

load_lookups_USER_TYPE = PythonOperator(
    task_id="load_lookup_USERTYPE",
    python_callable=load_s_lookups,
    op_kwargs={'lkp_name': 'USER_TYPE', 'lkp_data': {'auspraegung': ['User', 'Owner', ''], 'ID': [2, 1, 0]}},
    provide_context=True,
    dag=d
)

load_lookup_DURCHSCHNITTSALTER = PythonOperator(
    task_id="load_lookup_DURCHSCHNITTSALTER",
    python_callable=load_s_lookups,
    op_kwargs={'lkp_name': 'DURCHSCHNITTSALTER',
               'lkp_data': {'auspraegung': ['Female', 'Male', 'Div'], 'ID': [84, 79, 82]}},
    provide_context=True,
    dag=d
)

load_lookup_SEX = PythonOperator(
    task_id="load_lookup_SEX",
    python_callable=load_s_lookups,
    op_kwargs={'lkp_name': 'SEX', 'lkp_data': {'auspraegung': ['Female', 'Male', 'Div'], 'ID': [0, 1, 2]}},
    provide_context=True,
    dag=d
)

load_lookup_ANREDE = PythonOperator(
    task_id="load_lookup_ANREDE",
    python_callable=load_s_lookups,
    op_kwargs={'lkp_name': 'ANREDE',
               'lkp_data': {'auspraegung': ['Female', 'Male', 'Div'], 'ID': ['Frau', 'Herr', 'Mensch']}},
    provide_context=True,
    dag=d
)

load_lookup_CARTTYPE = PythonOperator(
    task_id="load_lookup_CARTTYPE",
    python_callable=load_d_lookups,
    op_kwargs={'lkp_name': 'CARTTYPE', 'tablename': 'card', 'column': 'card_type'},
    provide_context=True,
    dag=d
)

load_lookup_STATUS = PythonOperator(
    task_id="load_lookup_STATUS",
    python_callable=load_d_lookups,
    op_kwargs={'lkp_name': 'STATUS', 'tablename': 'loan', 'column': 'status'},
    provide_context=True,
    dag=d
)

load_lookup_cf_operation = PythonOperator(
    task_id="load_lookup_cf_operation",
    python_callable=load_d_lookups,
    op_kwargs={'lkp_name': 'cf_operation', 'tablename': 'trans', 'column': 'operation'},
    provide_context=True,
    dag=d
)

load_lookup_payment_type = PythonOperator(
    task_id="load_lookup_payment_type",
    python_callable=load_d_lookups,
    op_kwargs={'lkp_name': 'payment_type', 'tablename': 'trans', 'column': 'k_symbol'},
    provide_context=True,
    dag=d
)

src_dependency >> startAllTasks >> load_lookups_USER_TYPE >> load_lookup_DURCHSCHNITTSALTER >> load_lookup_SEX >> load_lookup_ANREDE >> load_lookup_CARTTYPE >> load_lookup_STATUS >> load_lookup_cf_operation >> load_lookup_payment_type >> endTasks
