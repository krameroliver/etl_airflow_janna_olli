import hashlib
import logging
import os
from datetime import datetime

import pandas as pd
import yaml
from dwhutils.lookup import get_lkp_value
from termcolor2 import colored

from dwhutils.ILoader import ILoader
from dwhutils.TableReader import read_raw_sql_sat
from dwhutils.db_connection import connect_to_db



class ORDER:
    def __init__(self, date):
        self.date = date
        self.date_dt = datetime.strptime(date, '%Y-%m-%d')
        self.schema_trg = 'biz'
        self.target = 'transaktion'
        self.schema_src = 'src'
        self.src_order = 'order'
        self.src_acct = 'acct'
        self.load_domain = self.__class__.__name__.upper()
        if os.path.isdir(r'/Configs/ENB/'):
            self.conf_r = r'/Configs/ENB/'
        else:
            self.conf_r = r'../Configs/ENB/'

    def join(self):
        trans = read_raw_sql_sat(db_con=connect_to_db(layer=self.schema_src), date=self.date, schema=self.schema_src,
                                 t_name=self.src_order)
        acct = read_raw_sql_sat(db_con=connect_to_db(layer=self.schema_src), date=self.date, schema=self.schema_src,
                                 t_name=self.src_acct)
        data = trans.merge(acct,how='left',on='account_id',suffixes=('','_'))


        return data

    def mapping(self, data: pd.DataFrame):
        with open(self.conf_r + self.target + '.yaml') as file:
            documents = yaml.full_load(file)
            print("mapping")
        sat_target_fields = documents[self.target]['tables']['s_' + self.target]['fields']
        sat_res_data = pd.DataFrame(columns=sat_target_fields)
        sat_res_data['transaktions_id'] = data['order_id']
        sat_res_data['ausfuehrungsdatum'] = data['parseddate']
        sat_res_data['betrag'] = data['amount']
        sat_res_data['loeschung'] = 10
        sat_res_data['buchungsart'] = 2
        sat_res_data['operation'] = 5
        sat_res_data['transaktion_hk'] = data['order_id'].apply(lambda x: hashlib.md5(x.encode()).hexdigest().upper())
        lkp = get_lkp_value(lkp_name='payment_type')
        sat_res_data['payment_type'] = data['k_symbol'].apply(
            lambda x: lkp[x])
        sat_res_data['load_domain'] = self.load_domain
        print("mapping done")
        return sat_res_data

    def writeToDB(self, data: pd.DataFrame):

        logging.info(colored('INFO: Entity ' + self.target, color='green'))

        con = connect_to_db(layer=self.schema_trg)

        loader = ILoader(date=self.date, loader_type='datavault',
                         loading_sat='s_transaktion',
                         loading_entity=self.target,
                         target_connection=con,
                         schema=self.schema_trg, build_hash_key=False,load_domain=self.load_domain)
        loader.load(data=data)


        print('--- Beladung Ende ---\n')


konto = ORDER('2018-12-31')
konto.writeToDB(konto.mapping(konto.join()))
