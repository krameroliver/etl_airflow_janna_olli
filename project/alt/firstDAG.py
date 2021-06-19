import hashlib
import os,sys
import pandas as pd
from sqlalchemy import create_engine
from datetime import timedelta, datetime

import pymysql
from sshtunnel import SSHTunnelForwarder
import time
import numpy as np


#from airflow import DAG
#from airflow.operators.python_operator import PythonOperator
#from airflow.operators.bash_operator import BashOperator

from utils.TechFields import add_technical_col
from utils.db_loader import load_to_db
from utils.loader import load
#
# default_args={
#         "owner":"airflow",
#         'retries':2,
#         'retry_delay':timedelta(minutes=0.5),
#         'start_date': datetime(2021,5,21)
#     }
#
# def readData(name:str,dele:str=None):
#     source_path = r"/rawdata/ENB/2018-12-31/"
#     data = pd.read_csv(os.path.join(source_path,name+'.csv'), delimiter=dele, header=0)
#
#     return data
#
#
# def filterData(data:pd.DataFrame):
#     data = data[ data [ 'run_id' ] != 1048573]
#     data = data.astype(str)
#     return data
#
#
# def writeToDB(name:str,dele:str=None):
#     tunnel = SSHTunnelForwarder(('82.165.203.114', 22), ssh_password="Z4ykW#&q*5", ssh_username="root",
#                                 remote_bind_address=("127.0.0.1", 3306))
#     tunnel.start()
#
#     con = pymysql.connect(host='127.0.0.1', user="dbuser", passwd="123456", port=tunnel.local_bind_port)
#
#     #con_s = "mysql+pymysql://dbuser:123456@82.165.203.114:22/src?charset=utf8mb4" #'postgresql://postgres:123456@OKRAMER-MAC:5432/BANK'
#     #con = create_engine(con_s, echo=False, pool_recycle=3600)
#     data = readData(name,dele)
#     print("ReadFinished")
#     print("write to temp table")
#     #load(data=data, db_con=con, t_name=name, date='2018-12-31', schema='src',commit_size=1000)
#     data.to_sql(name=name,con=con)
#
#
#     #data = add_technical_col(data=data,t_name=name,date=None)
#     #data = data.replace(np.nan,'NULL')
#     #data = data.sort_values('trans_id')
#     #data.drop_duplicates(inplace=True)
#     #print('loadDB')
#     #load_to_db(data=data, db_con=con, t_name=name, date='2018-12-31', schema='src',commit_size=1000)
#     #data.to_sql(name=name,con=con,if_exists='append',index=False)
#
#
#
#
# def writeToDBTrans():
#     source_path = r"/rawdata/ENB/2018-12-31/"
#     data = pd.read_csv(os.path.join(source_path, 'trans' + '.csv'), delimiter=';', header=1)
#     con_s = "mysql+pymysql://oliver:123456@OKRAMER-MAC/src?charset=utf8mb4"
#     con = create_engine(con_s, echo=False)
#     data.to_sql('trans', con, 'src', 'replace', False)
#
#
#
#
#
#
#
#
#
#
# #
# #
# # d = DAG(dag_id='load_source',
# #         schedule_interval="@daily",
# #         default_args=default_args,
# #         catchup=False)
# #
# # startAllTasks = BashOperator(
# #     task_id='start',
# #     bash_command='echo "Start All Tasks"',
# #     dag=d
# # )
# #
# # endTasks = BashOperator(
# #     task_id='end',
# #     bash_command='echo "All Tasks Finished"',
# #     dag=d
# # )
# # load_db_acct = PythonOperator(
# #     task_id="writeToDB_acct",
# #     python_callable=writeToDB,
# #     provide_context=True,
# #     op_kwargs={'name': 'acct'},
# #     dag=d
# #)
# # load_db_trans = PythonOperator(
# #     task_id="writeToDB_trans",
# #     python_callable=writeToDBTrans,
# #     provide_context=True,
# #     dag=d
# # )
# #
# #
# # load_db_card = PythonOperator(
# #     task_id="writeToDB_card",
# #     python_callable=writeToDB,
# #     provide_context=True,
# #     op_kwargs={'name': 'card'},
# #     dag=d
# # )
# #
# #
# # load_db_client = PythonOperator(
# #     task_id="writeToDB_client",
# #     python_callable=writeToDB,
# #     provide_context=True,
# #     op_kwargs={'name': 'client'},
# #     dag=d
# # )
# #
# #
# # load_db_disp = PythonOperator(
# #     task_id="writeToDB_disp",
# #     python_callable=writeToDB,
# #     provide_context=True,
# #     op_kwargs={'name': 'disposition'},
# #     dag=d
# # )
# #
# #
# # load_db_dist = PythonOperator(
# #     task_id="writeToDB_dist",
# #     python_callable=writeToDB,
# #     provide_context=True,
# #     op_kwargs={'name': 'district'},
# #     dag=d
# # )
# #
# #
# # load_db_loan = PythonOperator(
# #     task_id="writeToDB_loan",
# #     python_callable=writeToDB,
# #     provide_context=True,
# #     op_kwargs={'name': 'loan'},
# #     dag=d
# # )
# #
# #
# # load_db_order = PythonOperator(
# #     task_id="writeToDB_order",
# #     python_callable=writeToDB,
# #     provide_context=True,
# #     op_kwargs={'name': 'order'},
# #     dag=d
# # )
#
# #startAllTasks >> load_db_acct >> endTasks
#
#
# #startAllTasks >> [load_db_order,load_db_order,load_db_loan,load_db_dist,load_db_disp,load_db_acct,load_db_client,load_db_card,load_db_trans]
# #endTasks << [load_db_order,load_db_order,load_db_loan,load_db_dist,load_db_disp,load_db_acct,load_db_client,load_db_card,load_db_trans]
