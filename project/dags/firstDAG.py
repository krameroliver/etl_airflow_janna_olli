import hashlib
import os,sys
import pandas as pd
from sqlalchemy import create_engine
from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

from utils.TechFields import add_technical_col
from utils.db_loader import load_to_db

default_args={
        "owner":"airflow",
        'retries':2,
        'retry_delay':timedelta(minutes=0.5),
        'start_date': datetime(2021,5,21)
    }

def readData(name:str):
    source_path = r"../rawdata/ENB/2018-12-31/"
    data = pd.read_csv(os.path.join(source_path,name+'.csv'), delimiter=',', header=0)
    return data

def writeToDB(name:str):
    con_s = "mysql+pymysql://oliver:123456@OKRAMER-MAC/src?charset=utf8mb4" #'postgresql://postgres:123456@OKRAMER-MAC:5432/BANK'
    con = create_engine(con_s, echo=False)
    data = readData(name)
    data = add_technical_col(data=data,t_name=name,date=None)
    #data.drop_duplicates(inplace=True)
    #load_to_db(data=data, db_con=con, t_name=kwargs['name'], date='2018-12-31')
    data.to_sql(name=name,con=con,if_exists='append',index=False)




def writeToDBTrans():
    source_path = r"/rawdata/ENB/2018-12-31/"
    data = pd.read_csv(os.path.join(source_path, 'trans' + '.csv'), delimiter=';', header=1)
    con_s = "mysql+pymysql://oliver:123456@OKRAMER-MAC/src?charset=utf8mb4"
    con = create_engine(con_s, echo=False)
    data.to_sql('trans', con, 'src', 'replace', False)


writeToDB('acct')
#
#
# d = DAG(dag_id='load_source',
#         schedule_interval="@daily",
#         default_args=default_args,
#         catchup=False)
#
# startAllTasks = BashOperator(
#     task_id='start',
#     bash_command='echo "Start All Tasks"',
#     dag=d
# )
#
# endTasks = BashOperator(
#     task_id='end',
#     bash_command='echo "All Tasks Finished"',
#     dag=d
# )
# load_db_acct = PythonOperator(
#     task_id="writeToDB_acct",
#     python_callable=writeToDB,
#     provide_context=True,
#     op_kwargs={'name': 'acct'},
#     dag=d
#)
# load_db_trans = PythonOperator(
#     task_id="writeToDB_trans",
#     python_callable=writeToDBTrans,
#     provide_context=True,
#     dag=d
# )
#
#
# load_db_card = PythonOperator(
#     task_id="writeToDB_card",
#     python_callable=writeToDB,
#     provide_context=True,
#     op_kwargs={'name': 'card'},
#     dag=d
# )
#
#
# load_db_client = PythonOperator(
#     task_id="writeToDB_client",
#     python_callable=writeToDB,
#     provide_context=True,
#     op_kwargs={'name': 'client'},
#     dag=d
# )
#
#
# load_db_disp = PythonOperator(
#     task_id="writeToDB_disp",
#     python_callable=writeToDB,
#     provide_context=True,
#     op_kwargs={'name': 'disposition'},
#     dag=d
# )
#
#
# load_db_dist = PythonOperator(
#     task_id="writeToDB_dist",
#     python_callable=writeToDB,
#     provide_context=True,
#     op_kwargs={'name': 'district'},
#     dag=d
# )
#
#
# load_db_loan = PythonOperator(
#     task_id="writeToDB_loan",
#     python_callable=writeToDB,
#     provide_context=True,
#     op_kwargs={'name': 'loan'},
#     dag=d
# )
#
#
# load_db_order = PythonOperator(
#     task_id="writeToDB_order",
#     python_callable=writeToDB,
#     provide_context=True,
#     op_kwargs={'name': 'order'},
#     dag=d
# )

#startAllTasks >> load_db_acct >> endTasks


#startAllTasks >> [load_db_order,load_db_order,load_db_loan,load_db_dist,load_db_disp,load_db_acct,load_db_client,load_db_card,load_db_trans]
#endTasks << [load_db_order,load_db_order,load_db_loan,load_db_dist,load_db_disp,load_db_acct,load_db_client,load_db_card,load_db_trans]
