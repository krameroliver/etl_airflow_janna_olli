import hashlib
import os
from datetime import datetime

import pandas as pd
import yaml
from termcolor2 import colored

try:
    from utils.DataVaultLoader import DataVaultLoader
    from utils.TableReader import read_raw_sql_sat
    from utils.TechFields import add_technical_col
    from utils.db_connection import connect_to_db
    from utils.lookup import get_lkp_value
    from utils.ILoader import ILoader
except ImportError:
    from project.dags.utils.DataVaultLoader import DataVaultLoader
    from project.dags.utils.TableReader import read_raw_sql_sat
    from project.dags.utils.TechFields import add_technical_col
    from project.dags.utils.db_connection import connect_to_db
    from project.dags.utils.lookup import get_lkp_value
    from project.dags.utils.ILoader import ILoader


class Transaktion:
    def __init__(self, date):
        self.date = date
        self.date_dt = datetime.strptime(date, '%Y-%m-%d')
        self.schema_trg = 'biz'
        self.target = 'transaktion'
        self.schema_src = 'src'
        self.src_trans = 'trans'
        self.load_domain = self.__class__.__name__.upper()
        if os.path.isdir(r'/Configs/ENB/'):
            self.conf_r = r'/Configs/ENB/'
        else:
            self.conf_r = r'../Configs/ENB/'
        self.db_com_src = connect_to_db(layer=self.schema_src)
        self.db_com_trg = connect_to_db(layer=self.schema_trg)

    def join(self):
        trans = read_raw_sql_sat(db_con=self.db_com_src, date=self.date, schema=self.schema_src,
                                 t_name=self.src_trans)
        return trans

    def mapping(self, data: pd.DataFrame):
        with open(self.conf_r + self.target + '.yaml') as file:
            documents = yaml.full_load(file)
        sat_target_fields = documents[self.target]['tables']['s_' + self.target]['fields']
        sat_res_data = pd.DataFrame(columns=sat_target_fields)

        sat_res_data['transaktions_id'] = data['trans_id']
        sat_res_data['ausfuehrungsdatum'] = data['fulldate']
        sat_res_data['betrag'] = data['amount']
        sat_res_data['loeschung'] = 10
        sat_res_data['buchungsart'] = 1
        sat_res_data['transaktion_hk'] = data['trans_id'].apply(lambda x: hashlib.md5(x.encode()).hexdigest().upper())
        sat_res_data['load_domain'] = self.load_domain

        lkp_cf_opperation = get_lkp_value(lkp_name='CF_OPERATION')
        lkp_payment_type = get_lkp_value(lkp_name='PAYMENT_TYPE')
        lkp_payment_type[' '] = 99

        sat_res_data['operation'] = data['operation'].apply(
            lambda x: lkp_cf_opperation[x])
        sat_res_data['payment_type'] = data['k_symbol'].apply(
            lambda x: lkp_payment_type[x])
        return sat_res_data

    def writeToDB(self, data: pd.DataFrame):
        print(colored('INFO: Entity ' + self.target, color='green'))

        loader = ILoader(date=self.date, loader_type='datavault', loading_sat='s_transaktion',
                         loading_entity=self.target,
                         target_connection=self.db_com_trg,
                         schema=self.schema_trg, build_hash_key=False, load_domain=self.load_domain)
        loader.load(data=data)

        print('--- Beladung Ende ---\n')


konto = Transaktion('2018-12-31')
konto.writeToDB(konto.mapping(konto.join()))
