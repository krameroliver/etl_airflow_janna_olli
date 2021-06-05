import datetime
import os

import pandas as pd
# from Librarys.TechFiels import Techfields
import sqlalchemy as db
from sqlalchemy import MetaData, column
from sqlalchemy import delete, insert, update, select
from sqlalchemy.dialects import mysql
from sqlalchemy.dialects.mysql import insert
from termcolor2 import colored
import numpy as np
from project.dags.utils.db_connection import connect_to_db


def load_to_db(data: pd.DataFrame, db_con, t_name, date, schema:str=None,commit_size:int=10000):
    results_i = None
    results_u = None
    results_d = None
    target_table = None
    #target_table_hist = None

    metadata = MetaData(bind=db_con)
    metadata.reflect(bind=db_con, schema=schema)

    for table in [i for i in reversed(metadata.sorted_tables) if i.name == t_name]:
        target_table = table
        for c in target_table.columns:
            if 'card_hk' in c.name:
                col=c

    # print(metadata.tables[target_table.name])
    #table_hk = list(pd.read_sql(sql='select {col}_hk from {schema}.{table};'.format(col=t_name, table=t_name, schema=schema), con=db_con)[t_name + '_hk'])
    stmt = (select([col]))

    table_hk=list(db_con.execute(stmt).fetchall())
    table_hk=[i[0] for i in table_hk]
    print(table_hk)

    df_hk = list(data[t_name + '_hk'])
    insert_hk = [i for i in df_hk if i not in table_hk]
    print(colored('INFO: insert: ' + str(len(insert_hk)), 'green'))
    delete_hk = [i for i in table_hk if i not in df_hk]
    print(colored('INFO: delete:' + str(len(delete_hk)), 'green'))
    update_hk = [i for i in table_hk if i in df_hk]
    print(colored('INFO: update:' + str(len(update_hk)), 'green'))

    insert_df=data.loc[data[t_name+'_hk'].isin(insert_hk)]
    delete_df = data.loc[data[t_name + '_hk'].isin(delete_hk)]
    update_df = data.loc[data[t_name + '_hk'].isin(update_hk)]

    conn = db_con.connect()
    #insert:
    for row in range(insert_df.shape[0]):
        record=insert_df.iloc[row]
        #print(record)
        stmt = (insert(target_table).values(record))

        conn.execute(stmt)
        if row % commit_size == 0:
            conn.execute('commit')

    conn.execute('commit')

    #delete:
    #for row in range(delete_df.shape[0]):
        #record=delete_df.iloc[row]
    #delete_hk=tuple(delete_hk)
    stmt = (delete(target_table).where(col == delete_hk))
    d_hk="('"+"','".join(delete_hk)+"')"
    print(datetime.date.today().strftime("%Y-%m-%d"))
    stmt2="DELETE FROM "+ schema+"."+target_table.name+" FOR PORTION OF business_time FROM '" \
          +datetime.date.today().strftime("%Y-%m-%d") + "' TO '2262-04-11' WHERE "+ t_name+'_hk'+" IN "+ d_hk+";"
    #stmt2=stmt+ "for portion of business_time from '" +datetime.date.today() + "' to " + "'2262-04-11';"
    #print(stmt.compile(dialect=mysql.dialect()))
    conn.execute(stmt2)







    conn.close()
    print('DB closed')








con=connect_to_db(layer="src")

source_path = r"../../rawdata/ENB/2018-12-31/"
data = pd.read_csv(os.path.join(source_path, 'card' + '.csv'), delimiter=',', header=0)
data["card_hk"]=data['card_id']
#data=data.iloc[0:10,:]
load_to_db(data,con,"card","2018-12-31","src")



    #
    # df_hk = list(data[t_name + '_hk'])
    #
    # insert_hk = [i for i in df_hk if i not in table_hk]
    # print(colored('INFO: insert: ' + str(len(insert_hk)), 'green'))
    # delete_hk = [i for i in table_hk if i not in df_hk]
    # print(colored('INFO: delete:' + str(len(delete_hk)), 'green'))
    # update_hk = [i for i in table_hk if i in df_hk]
    # print(colored('INFO: update:' + str(len(update_hk)), 'green'))
    # # values_list_i = data[data[t_name + '_hk'].isin(insert_hk)].to_dict('records')
    # # print(data[data[t_name+'_hk'] in insert_hk])
    #
    # data_list = []
    # expected_rows = commit_size
    # i = 0
    # j = expected_rows
    # rows = data.shape[0]
    # if rows > expected_rows:
    #     chunks = int(np.ceil(rows/expected_rows))
    #     for x in range(chunks):
    #         df_sliced = data[i:j]
    #         data_list.append(df_sliced)
    #
    #         i += expected_rows
    #         j += expected_rows
    # else:
    #     data_list.append(data)
    #
    # if len(insert_hk) > 0:
    #     for _data in data_list:
    #         values_list_i = _data[_data[t_name + '_hk'].isin(insert_hk)].to_dict('records')
    #     # insert
    #
    #         insrt_stmnt = insert(target_table).values(values_list_i)
    #         results_i = db_con.execute(insrt_stmnt)
    #         # autocommit
    #         #print('commit')
    #         db_con.execute('commit;')
    # else:
    #     print(colored('INFO: Keine Insert-Saetze vorhanden', color='yellow'))
    #
    # # delete
    # if len(delete_hk) > 0:
    #     delete_stmnt = delete(target_table).where(t_name + 'hk' in delete_hk)
    #     results_d = db_con.execute(delete_stmnt)
    # else:
    #     print(colored('INFO: Keine Delete-Saetze vorhanden', color='yellow'))
    #
    # # update
    # if len(update_hk) > 0:
    #     table_diff_hk = pd.read_sql(
    #         sql='select {col}_hk, diff_hk from {schema}.{table};'.format(col=t_name, table=t_name, schema=schema),
    #         con=db_con)
    #     df_diff_hk = data[[t_name + '_hk', 'diff_hk']]
    #
    #     table_diff_hk = table_diff_hk[table_diff_hk[t_name + '_hk'].isin(update_hk)]
    #     df_diff_hk = df_diff_hk[df_diff_hk[t_name + '_hk'].isin(update_hk)]
    #     merge_hk = pd.merge(table_diff_hk, df_diff_hk, how='inner', on=t_name + '_hk')
    #     merge_hk = merge_hk[merge_hk.apply(lambda x: x['diff_hk_x'] != x['diff_hk_y'], axis=1)]
    #     update_hk = list(merge_hk[t_name + '_hk'])
    #     if len(update_hk) > 0:
    #         data['mod_flg'] = 'U'
    #         data = data[data[t_name + '_hk'].isin(update_hk)]
    #         data.drop(inplace=True, columns=['record_source', 'processing_date_end'])
    #         values_list_u = data.to_dict('records')
    #
    #         for i in update_hk:
    #             update_stmnt = db.update(target_table).values(values_list_u[t_name + '_hk' == i]).where(
    #                 getattr(target_table.c, t_name + '_hk') == i)
    #             db_con.execute(update_stmnt)
    #
    #             #update_stmnt_hist = db.update(target_table_hist).values({'processing_date_end': date}).where(
    #              #   getattr(target_table_hist.c, t_name + '_hk') == i)
    #             #results_u = db_con.execute(update_stmnt_hist)
    #     else:
    #         print(colored('INFO: Keine Update-Saetze vorhanden', color='yellow'))
    # else:
    #     print(colored('INFO: Keine potentiellen Update-Saetze vorhanden', color='yellow'))
    #
    # return results_i, results_d, results_u
    #
