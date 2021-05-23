import pandas as pd
# from Librarys.TechFiels import Techfields
import sqlalchemy as db
from sqlalchemy import MetaData
from termcolor2 import colored
import numpy as np
from datetime import datetime



def readTableFromDB(db_con, t_name:str, date:str, schema:str):
    hist_table_name = t_name+"_hist"

    select_main = "select * from {schema}.{table};".format(schema=schema,table=t_name)
    select_hist = "select * from {schema}.{table};".format(schema=schema, table=hist_table_name)

    main_table = pd.read_sql(con=db_con,sql=select_main)
    hist_table = pd.read_sql(con=db_con,sql=select_hist)


    in_data = pd.concat([main_table,hist_table])
    #in_data['processing_date_end'] = pd.to_datetime(in_data['processing_date_end']).dt.strftime('%Y-%m-%d')
    in_data['processing_date_end'] = in_data['processing_date_end'].apply(pd.to_datetime)
    in_data['processing_date_start'] = in_data['processing_date_start'].apply(pd.to_datetime)
    in_data = in_data[in_data['processing_date_start'] <= date]
    in_data = in_data[in_data['processing_date_end'] > date]

    return in_data





