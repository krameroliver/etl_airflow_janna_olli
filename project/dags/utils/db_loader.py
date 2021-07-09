import datetime
import hashlib
import os

import pandas as pd
import yaml
# from Librarys.TechFiels import Techfields
from sqlalchemy import MetaData
from sqlalchemy import select
from termcolor2 import colored

try:
    from utils.utils import divide_chunks
except ImportError:
    from project.dags.utils.utils import divide_chunks


class LoadtoDB():
    def __init__(self, data: pd.DataFrame, db_con, t_name, date: str = None, schema: str = None,
                 commit_size: int = 10000, entityName: str = None):
        self.data = data
        self.db_con = db_con
        self.t_name = t_name
        self.schema = schema
        self.commit_size = commit_size
        self.target_table = None
        self.entityName = entityName

        if date == None:
            self.processing_date_start = datetime.date.today().strftime("%Y-%m-%d")
        else:
            self.processing_date_start = date

        if os.path.isdir(r'/Configs/ENB/'):
            conf_r = r'/Configs/ENB/'
        else:
            conf_r = r'../Configs/ENB/'

        if entityName == None:
            with open(conf_r + t_name + '.yaml') as file:
                self.documents = yaml.full_load(file)
            self.hk = self.documents[t_name]['hash_key']
            self.fields = self.documents[t_name]['fields']
        else:
            with open(conf_r + self.entityName + '.yaml') as file:
                self.documents = yaml.full_load(file)
            self.hk = self.documents[entityName]['tables'][self.t_name]['hash_key']
            self.fields = self.documents[entityName]['tables'][self.t_name]['fields']

        self.metadata = MetaData(bind=self.db_con)
        self.metadata.reflect(bind=self.db_con, schema=self.schema)

        for table in [i for i in reversed(self.metadata.sorted_tables) if self.t_name == i.name]:
            self.target_table = table
            for c in self.target_table.columns:
                if self.hk in c.name:
                    col = c

        stmt = (select([col]))

        table_hk = list(db_con.execute(stmt).fetchall())
        table_hk = [i[0] for i in table_hk]

        df_hk = list(self.data[self.hk])
        self.insert_hk = [i for i in df_hk if i not in table_hk]
        self.delete_hk = [i for i in table_hk if i not in df_hk]
        self.update_hk = [i for i in table_hk if i in df_hk]

        self.insert_df = data.loc[data[self.hk].isin(self.insert_hk)]
        self.delete_df = data.loc[data[self.hk].isin(self.delete_hk)]
        self.update_df = data.loc[data[self.hk].isin(self.update_hk)]

    def __repr__(self):
        repr_str = 'table: ' + self.target_table.name + '\n' \
                   + colored('INFO: insert: ' + str(len(self.insert_hk)), 'green') + '\n' \
                   + colored('INFO: delete:' + str(len(self.delete_hk)), 'green') + '\n' \
                   + colored('INFO: updates:' + str(self.update.shape[0]), 'green')

        return (repr_str)

    def insert(self):
        conn = self.db_con.connect()
        # insert:
        print('Start Insert:')
        self.insert_df['processing_date_start'] = self.processing_date_start
        _anz = 1
        record_list = self.insert_df.to_dict('record')
        chunks = divide_chunks(record_list, self.commit_size)

        for i in chunks:
            self.target_table.name = self.t_name
            conn.execute(self.target_table.insert(), i)

        conn.execute('commit')

        conn.close()
        print('Ende Insert: DB closed')

    def delete(self):
        conn = self.db_con.connect()
        print('Start Delete:')
        d_hk = "('" + "','".join(self.delete_hk) + "')"
        stmt2 = "DELETE FROM " + self.schema + "." + self.target_table.name + " FOR PORTION OF business_time FROM '" \
                + datetime.date.today().strftime(
            "%Y-%m-%d") + "' TO '2262-04-11' WHERE " + self.hk + " IN " + d_hk + ";"
        conn.execute(stmt2)
        conn.close()
        print('Ende Delete: DB closed')

    def vergelich(self, x):
        if x["diff_hk"] == self.diff_hk_lkp[x[self.hk]]:
            return 1
        else:
            return 0

    def update_v2(self):
        print('Start Update:')
        conn = self.db_con.connect()
        fields = list(self.fields)

        fields_with_hk = fields
        fields_with_hk.append(self.documents[self.entityName]['tables'][self.t_name]['hash_key'])
        fields_types = self.documents[self.entityName]['tables'][self.t_name]['data_types']
        fields_types.append('str')

        rel_types = {}
        parse_list = []
        for k, i in enumerate(fields):
            if fields_types[k] != 'DATUM':
                rel_types[i] = fields_types[k]
            else:
                rel_types[i] = 'str'
                parse_list.append(i)

        table_u_hk = "('" + "','".join(self.update_hk) + "')"
        stmt_diffs = "select  " + ','.join(
            fields_with_hk) + " from " + self.schema + "." + self.target_table.name + " where " + self.hk + " in " + table_u_hk + \
                     " and processing_date_end='2262-04-11';"

        old_data = pd.DataFrame(columns=fields_with_hk, data=conn.execute(stmt_diffs).fetchall()).astype(
            dtype=rel_types)
        old_data[parse_list] = old_data[parse_list].apply(lambda x: pd.to_datetime(x), axis=1)
        merged_df = self.update_df.merge(old_data, how='inner', on=self.hk, suffixes=('', '_'))
        diff_hks = []
        for hk in list(self.update_df[self.hk]):
            for c in fields:
                if c not in ['trans_hk']:
                    _temp = merged_df[merged_df[self.hk] == hk]
                    try:
                        _t1 = round(_temp[c].values[0], 0)
                        _t2 = round(_temp[c + '_'].values[0], 0)
                    except:
                        _t1 = _temp[c].values[0]
                        _t2 = _temp[c + '_'].values[0]

                    if _t1 != _t2:
                        diff_hks.append(hk)

        update_df = self.update_df[self.update_df[self.hk].isin(diff_hks)]
        k = [i for i in fields if i != self.hk]
        update_df['diff_str'] = update_df[k].astype(str).agg('|'.join, axis=1)
        update_df["diff_hk"] = update_df['diff_str'].astype(str).apply(
            lambda x: hashlib.md5(x.encode()).hexdigest().upper())
        if update_df.shape[0] > 0:
            for row in range(update_df.shape[0]):
                record = update_df.iloc[row]
                u_hk = record[self.hk]
                set_part = []
                for col in fields:
                    set_part.append(self.target_table.name + "." + col + "=" + "'" + str(record[col]) + "'")
                set_part = ",\n".join(set_part)

                stmt3 = "UPDATE " + self.schema + "." + self.target_table.name + " FOR PORTION OF business_time FROM '" + self.processing_date_start + "' TO '2262-04-11' SET " + set_part + ", mod_flg = 'U'  WHERE " + self.hk + " = '" + u_hk + "';"

                conn.execute(stmt3)
                if row % self.commit_size == 0:
                    conn.execute('commit')
                if row % 100 == 0:
                    print('.', end=' ')
        print('Ende Update: DB closed')

    def update(self):
        conn = self.db_con.connect()
        print('Start Update:')
        fields = self.fields  # self.documents[self.target_table.name]['fields']
        fields.append('diff_hk')
        table_u_hk = "('" + "','".join(self.update_hk) + "')"
        stmt_diffs = "select diff_hk, " + self.hk + " from " + self.schema + "." + self.target_table.name + " where " + self.hk + " in " + table_u_hk + \
                     " and processing_date_end='2262-04-11';"
        table_u_list = self.db_con.execute(stmt_diffs).fetchall()
        table_u_df = pd.DataFrame(columns=['diff_hk', self.hk])
        for i in table_u_list:
            table_u_df = table_u_df.append({'diff_hk': i[0], self.hk: i[1]}, ignore_index=True)

        self.update_df['processing_date_start'] = self.processing_date_start  # update auf bestimmtes datum
        self.update_df['diff_str'] = self.update_df.astype(str).agg('|'.join, axis=1)
        self.update_df["diff_hk"] = self.update_df['diff_str'].astype(str).apply(
            lambda x: hashlib.md5(x.encode()).hexdigest().upper())
        self.update_df.drop(inplace=True, columns='diff_str')

        join_update = self.update_df.merge(table_u_df, how='inner', on=self.hk, suffixes=['', '_tudf'])

        self.update = join_update[join_update['diff_hk'] != join_update['diff_hk_tudf']]
        self.update.to_csv(r'/Users/oliver/eclipse-workspace/etl_airflow_janna_olli/project/tempdata/diff_hk.csv')
        # print(update)
        print(colored('INFO: update:' + str(self.update.shape[0]), 'green'))

        if self.update.shape[0] > 0:
            for row in range(self.update.shape[0]):
                record = self.update.iloc[row]
                u_hk = record[self.hk]
                set_part = []
                for col in fields:
                    set_part.append(self.target_table.name + "." + col + "=" + "'" + str(record[col]) + "'")
                set_part = ",\n".join(set_part)
                # print(set_part)
                stmt3 = "UPDATE " + self.schema + "." + self.target_table.name + " FOR PORTION OF business_time FROM '" + self.processing_date_start + "' TO '2262-04-11' SET " + set_part + ", mod_flg = 'U'  WHERE " + self.hk + " = '" + u_hk + "';"

                # stmt3 = "UPDATE " + self.schema + "." + self.target_table.name + " FOR PORTION OF business_time FROM '" + datetime.date.today().strftime(
                #    "%Y-%m-%d") + "' TO '2262-04-11' SET " + set_part + ", mod_flg = 'U'  WHERE " + self.hk + " = '" + u_hk + "';"

                conn.execute(stmt3)
                if row % self.commit_size == 0:
                    conn.execute('commit')
                if row % 100 == 0:
                    print('.', end=' ')

        conn.close()
        print('Ende Update: DB closed')
