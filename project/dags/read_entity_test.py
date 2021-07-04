import yaml
from utils.TableReader import read_raw_sql_sat, read_raw_sql_hub
from utils.db_connection import connect_to_db
import pandas as pd

with open(r'./Configs/ENB/geschaeftspartner.yaml') as file:
    documents = yaml.full_load(file)

con = connect_to_db("biz")

tables = sorted(documents['geschaeftspartner']['tables'].keys())
hashkey = documents['geschaeftspartner']['tables'][tables[0]]['hash_key']
tablelist = {}
for table in tables:
    if "h_" in table:
        tablelist[table]=read_raw_sql_hub(db_con=con, t_name=table, date='2018-12-31', schema='biz')
    elif "s_" in table or "m_" in table:
        tablelist[table]=read_raw_sql_sat(db_con=con, t_name=table, date='2018-12-31', schema='biz')

end_df = pd.DataFrame()
for t in tablelist.keys():
    try:
        end_df = end_df.merge(right=tablelist[t], how='inner', on=hashkey)
    except:
        print('table {t} could not be joined because its empty'.format(t=t))
