import os,sys
import pandas as pd
from sqlalchemy import create_engine
from datetime import timedelta, datetime

default_args={
        "owner":"airflow",
        'retries':1,
        'retry_delay':timedelta(minutes=5),
        'start_date': datetime(2021,5,16)
    }

def readData(**kwargs):
    source_path = r"/rawdata/ENB/2018-01-31/"
    data = pd.read_csv(os.path.join(source_path,kwargs['name']+'.csv'), delimiter=',', header=1)
    task_instance = kwargs['task_instance']
    task_instance.xcom_push(key=kwargs['name'], value=data)
    return data

def writeToDB(**kwargs):
    con_s = 'postgresql://postgres:123456@OKRAMER-MAC:5432/BANK'
    con = create_engine(con_s, echo=False)
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='readData', key=kwargs['name'])
    data.to_sql(kwargs['name'],con,'src','replace',False)
