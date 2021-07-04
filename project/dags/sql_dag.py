import os

from airflow import DAG
from airflow.lineage import LineageBackend
from sqlalchemy import MetaData

from sqlalchemy import text
from utils.db_connection import connect_to_db
from airflow.operators.python_operator import PythonOperator

default_arg = {'owner': 'airflow', 'start_date': '2021-06-20'}



def drop(layer:str=None):
    con = connect_to_db(layer=layer)
    metadata = MetaData(bind=con)
    metadata.reflect(bind=con, schema=layer)
    for table in [i for i in reversed(metadata.sorted_tables)]:
        con.execute("DROP TABLE {schema}.{table};".format(schema=layer,table=table.name))
        con.execute("commit")
    #con.close()


def run_sql(sql_file:str=None,layer:str=None):
    con = connect_to_db(layer=layer)
    metadata = MetaData(bind=con)
    metadata.reflect(bind=con, schema=layer)

    file = open(sql_file, 'r')
    lines = os.linesep.join(file.readlines())
    sqls = lines.split('--delim')
    for sql in sqls:
        con.execute(sql)
        con.execute("commit")
    con.execute("commit")

dag = DAG(dag_id='bootstrap',
        schedule_interval="@daily",
          default_args=default_arg)

src_conn = connect_to_db("src")

drop_src = PythonOperator(
    task_id="drop_src",
    python_callable=drop,
    op_kwargs={ 'layer':'src'},
    dag=dag
)

create_src = PythonOperator(
    task_id="create_src",
    python_callable=run_sql,
    op_kwargs={'sql_file': '/SQLS/src.sql', 'layer':'src'},
    dag=dag
)



drop_src >> create_src