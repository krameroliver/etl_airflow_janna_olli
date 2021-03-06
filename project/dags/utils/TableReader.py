import pandas as pd
# from Librarys.TechFiels import Techfields
import sqlalchemy as db
from sqlalchemy import MetaData
from termcolor2 import colored
import numpy as np
from datetime import datetime, date


def read_raw_sql_hub(db_con, t_name: str, date: str, schema: str):
    metadata = MetaData(bind=db_con)
    metadata.reflect(bind=db_con, schema=schema)

    for table in [i for i in reversed(metadata.sorted_tables) if i.name == t_name]:
        target_table = table
    cols = target_table.columns
    cols = [i.name.replace(t_name + ".", "") for i in cols]
    data = db_con.execute(target_table.select()).fetchall()
    df = pd.DataFrame(columns=cols, data=data)
    df = df.reset_index()
    return df


def read_raw_sql_sat(db_con, t_name: str, date: str, schema: str):
    print(colored("Lese tabelle {0}".format(t_name)))
    metadata = MetaData(bind=db_con)
    metadata.reflect(bind=db_con, schema=schema)

    for table in [i for i in reversed(metadata.sorted_tables) if i.name == t_name]:
        target_table = table

    date_date = datetime.strptime(date, '%Y-%m-%d').date()
    cols = target_table.columns
    cols = [i.name.replace(t_name + ".", "") for i in cols]
    data = db_con.execute(target_table.select()).fetchall()
    df = pd.DataFrame(columns=cols, data=data)
    df = df[df['processing_date_start'] <= date_date]
    df = df[df['processing_date_end'] > date_date]
    df = df.reset_index()
    print(colored('es wurden {0} datensaetze aus der Tabelle {1} gelesen'.format(df.shape[0], t_name)))
    return df


def readTableFromDB(db_con, t_name: str, date: str, schema: str):
    hist_table_name = t_name + "_hist"

    select_main = "select * from {schema}.{table};".format(schema=schema, table=t_name)
    select_hist = "select * from {schema}.{table};".format(schema=schema, table=hist_table_name)

    # main_table = pd.read_sql(con=db_con,sql=select_main)
    # hist_table = pd.read_sql(con=db_con,sql=select_hist)

    # in_data = pd.concat([main_table,hist_table])
    # in_data['processing_date_end'] = pd.to_datetime(in_data['processing_date_end']).dt.strftime('%Y-%m-%d')
    # in_data['processing_date_end'] = in_data['processing_date_end'].apply(pd.to_datetime)
    # in_data['processing_date_start'] = in_data['processing_date_start'].apply(pd.to_datetime)
    # in_data = in_data[in_data['processing_date_start'] <= date]
    # in_data = in_data[in_data['processing_date_end'] > date]

    # return in_data
