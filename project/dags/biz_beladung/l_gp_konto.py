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
    from utils.lookup import get_lkp_value
    from utils.ILoader import ILoader
except ImportError:
    from project.dags.utils.DataVaultLoader import DataVaultLoader
    from project.dags.utils.TableReader import read_raw_sql_sat
    from project.dags.utils.TechFields import add_technical_col
    from project.dags.utils.db_connection import connect_to_db
    from project.dags.utils.lookup import get_lkp_value
    from project.dags.utils.ILoader import ILoader


class Link_GP_KONTO:
    def __init__(self, date):
        self.date = date
        self.date_dt = datetime.strptime(date, '%Y-%m-%d')
        self.schema_trg = 'biz'
        self.target = 'gp_konto'
        self.schema_src = 'src'
        self.src_acct = 'acct'
        self.load_domain = self.__class__.__name__.upper()
        self.src_disp = 'disposition'
        self.src_client = 'client'
        if os.path.isdir(r'/Configs/ENB/'):
            self.conf_r = r'/Configs/ENB/'
        else:
            self.conf_r = r'../Configs/ENB/'

    def join(self):
        acct = read_raw_sql_sat(db_con=connect_to_db(layer=self.schema_src), date=self.date, schema=self.schema_src,
                                t_name=self.src_acct)

        disp = read_raw_sql_sat(db_con=connect_to_db(layer=self.schema_src), date=self.date, schema=self.schema_src,
                                t_name=self.src_disp)

        client = read_raw_sql_sat(db_con=connect_to_db(layer=self.schema_src), date=self.date, schema=self.schema_src,
                                  t_name=self.src_client)

        out_data = client.merge(disp, how='inner', left_on='client_id', right_on='client_id',
                                suffixes=('', '_disp')).merge(
            acct, how='inner', left_on='account_id', right_on='account_id', suffixes=('', '_card'))

        out_data = out_data[['account_id', 'client_id', 'user_type']]

        return out_data

    def mapping(self, data: pd.DataFrame):
        lkp_cf_opperation = get_lkp_value(lkp_name='USER_TYPE')

        out_data = pd.DataFrame()
        out_data['konto_hk'] = data['account_id'].apply(lambda x: hashlib.md5(x.encode()).hexdigest().upper())
        out_data['geschaeftspartner_hk'] = data['client_id'].apply(
            lambda x: hashlib.md5(x.encode()).hexdigest().upper())
        out_data['gp_rolle'] = data['user_type'].apply(lambda x: lkp_cf_opperation[x])
        out_data['load_domain'] = self.load_domain
        return out_data

    def writeToDB(self, data: pd.DataFrame):
        logging.info(colored('INFO: Entity ' + self.target, color='green'))
        con = connect_to_db(layer=self.schema_trg)

        loader = ILoader(date=self.date, loader_type='datavault',
                         loading_sat='l_m_gp_konto',
                         loading_entity=self.target,
                         target_connection=con,
                         schema=self.schema_trg, build_hash_key=True, load_domain=self.load_domain)
        loader.load(data=data)

        logging.info('--- Beladung Ende ---\n')


konto = Link_GP_KONTO('2018-12-31')
konto.writeToDB(konto.mapping(konto.join()))
