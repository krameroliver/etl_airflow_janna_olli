from utils.DataVaultLoader import DataVaultLoader
from utils.TechFields import add_technical_col
import pandas as pd
import os
import numpy as np

from utils.db_connection import connect_to_db


def read_write_source(file ,date, table, delm, header):
    source_path = r"/rawdata/ENB/"
    source_path = os.path.join(source_path,date,file)
    data=pd.read_csv(source_path, delimiter=delm, header=header)
    data.fillna(value="",inplace=True)
    data=add_technical_col(data = data,t_name = table,date=date,entity_name = table)

    con = connect_to_db(layer="src")
    dvl=DataVaultLoader(data=data, t_name=table, date=date,db_con=con,entity_name=table ,schema='src',commit_size=1000)
    dvl.load

