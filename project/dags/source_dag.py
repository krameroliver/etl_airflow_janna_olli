from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta, datetime
from project.dags.source_beladung.source_load import read_write_source

default_args={
        "owner":"airflow",
        'retries':2,
        'retry_delay':timedelta(minutes=0.5),
        'start_date': datetime(2021,6,12)
    }

d = DAG(dag_id='load_all_source',
        schedule_interval="@daily",
        default_args=default_args,
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

load_db_acct = PythonOperator(
    task_id="load_acct",
    python_callable=read_write_source,
    provide_context=True,
    op_kwargs={'file': 'acct.csv', 'date': "2018-12-31", 'table': 'acct', 'header': 0, 'delm': ','},
    dag=d
)

load_db_card = PythonOperator(
    task_id="load_card",
    python_callable=read_write_source,
    provide_context=True,
    op_kwargs={'file': 'card.csv', 'date': "2018-12-31", 'table': 'card', 'header': 0, 'delm': ','},
    dag=d
)
load_db_client = PythonOperator(
    task_id="load_client",
    python_callable=read_write_source,
    provide_context=True,
    op_kwargs={'file': 'client.csv', 'date': "2018-12-31", 'table': 'client', 'header': 0, 'delm': ','},
    dag=d
)




startAllTasks >> [load_db_acct,load_db_card,load_db_client] >> endTasks
