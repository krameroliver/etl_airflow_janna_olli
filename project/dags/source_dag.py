from airflow import DAG
from airflow.lineage import LineageBackend
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta, datetime
from source_beladung.source_load import read_write_source

default_args={
        "owner":"airflow",
        'retries':0‚,
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

load_db_disposition = PythonOperator(
    task_id="load_disposition",
    python_callable=read_write_source,
    provide_context=True,
    op_kwargs={'file': 'disposition.csv', 'date': "2018-12-31", 'table': 'disposition', 'header': 0, 'delm': ','},
    dag=d
)
load_db_district = PythonOperator(
   task_id="load_district",
   python_callable=read_write_source,
   provide_context=True,
   op_kwargs={'file': 'district.csv', 'date': "2018-12-31", 'table': 'district', 'header': 0, 'delm': ','},
   dag=d
)
load_db_loan = PythonOperator(
   task_id="load_loan",
   python_callable=read_write_source,
   provide_context=True,
   op_kwargs={'file': 'loan.csv', 'date': "2018-12-31", 'table': 'loan', 'header': 0, 'delm': ','},
   dag=d
)

load_db_order = PythonOperator(
    task_id="load_order",
    python_callable=read_write_source,
    provide_context=True,
    op_kwargs={'file': 'order.csv', 'date': "2018-12-31", 'table': 'order', 'header': 0, 'delm': ','},
    dag=d
)

load_db_trans = PythonOperator(
    task_id="load_trans",
    python_callable=read_write_source,
    provide_context=True,
    op_kwargs={'file': 'trans.csv', 'date': "2018-12-31", 'table': 'trans', 'header': 0, 'delm': ';'},
    dag=d
)

startAllTasks >> [load_db_acct,load_db_card,load_db_client,load_db_trans,load_db_order,load_db_loan,load_db_district,load_db_disposition] >> endTasks
#startAllTasks >> load_db_acct >> endTasks