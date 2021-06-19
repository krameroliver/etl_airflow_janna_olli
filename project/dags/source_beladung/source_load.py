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
    data = data.replace(np.nan, 'Null')
    data = data.fillna('Null')
    data=add_technical_col(data,table)

    con = connect_to_db(layer="src")
    dvl=DataVaultLoader(data,con,table,date,"src")
    dvl.load
    print(dvl)


#read_write_source("card.csv","2018-12-31","card",",",0)
#read_write_source("acct.csv","2018-12-31","acct",",",0)