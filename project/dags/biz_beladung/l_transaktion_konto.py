import hashlib
import os
from datetime import datetime
import pandas as pd
import yaml
from termcolor import colored
import logging

try:
    from utils.DataVaultLoader import DataVaultLoader
    from utils.TableReader import read_raw_sql_sat
    from utils.TechFields import add_technical_col
    from utils.db_connection import connect_to_db
    from utils.ILoader import ILoader
except ImportError:
    from project.dags.utils.DataVaultLoader import DataVaultLoader
    from project.dags.utils.TableReader import read_raw_sql_sat
    from project.dags.utils.TechFields import add_technical_col
    from project.dags.utils.db_connection import connect_to_db
    from project.dags.utils.ILoader import ILoader


class Link_TRANS_KONTO:
    def __init__(self, date):
        self.date = date
        self.date_dt = datetime.strptime(date, '%Y-%m-%d')
        self.schema_trg = 'biz'
        self.target = 'trans_konto'
        self.schema_src = 'src'
        self.src_trans = 'trans'
        self.src_order = 'order'
        self.src_acct = 'acct'
        if os.path.isdir(r'/Configs/ENB/'):
            self.conf_r = r'/Configs/ENB/'
        else:
            self.conf_r = r'../Configs/ENB/'

    def join(self):
        trans = read_raw_sql_sat(db_con=connect_to_db(layer=self.schema_src), date=self.date, schema=self.schema_src,
                                t_name=self.src_trans)

        trans = trans[['trans_id','account_id']]
        trans['trans_type'] = 1

        order = read_raw_sql_sat(db_con=connect_to_db(layer=self.schema_src), date=self.date, schema=self.schema_src,
                                 t_name=self.src_order)
        order = order[['order_id','account_id']]
        order['trans_type'] = 1
        order['trans_id'] = order['order_id'].apply(lambda x : str(x))
        order = order.drop('order_id',axis=1)






        out_data = pd.concat([trans,order])
        out_data = out_data[out_data.notnull()]
        return out_data

    def mapping(self,data:pd.DataFrame):
        out_data = pd.DataFrame()
        out_data['transaktion_hk'] = data['trans_id'].apply(lambda x: hashlib.md5(x.encode()).hexdigest().upper())
        out_data['konto_hk'] = data['account_id'].apply(lambda x: hashlib.md5(x.encode()).hexdigest().upper())
        out_data['trans_type'] = data['trans_type']

        return out_data

    def writeToDB(self, data: pd.DataFrame):
        logging.info(colored('INFO: Entity ' + self.target, color='green'))

        con = connect_to_db(layer=self.schema_trg)

        loader = ILoader(date=self.date, loader_type='datavault',
                         loading_sat='l_s_trans_konto',
                         loading_entity=self.target,
                         target_connection=con,
                         schema=self.schema_trg)
        loader.load(data=data)

        logging.info('--- Beladung Ende ---\n')


entity = Link_TRANS_KONTO(date='2018-12-31')
entity.writeToDB(entity.mapping(entity.join()))