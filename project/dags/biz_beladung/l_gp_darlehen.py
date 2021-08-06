import hashlib
import os
from datetime import datetime
import pandas as pd
import yaml
from attr._make import _has_own_attribute
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


class Link_GP_DARLEHEN:
    def __init__(self, date):
        self.date = date
        self.date_dt = datetime.strptime(date, '%Y-%m-%d')
        self.schema_trg = 'biz'
        self.target = 'gp_darlehen'
        self.schema_src = 'src'
        self.src_loan = 'loan'
        self.src_acct = 'acct'
        self.src_client = 'client'
        self.src_disp = 'disposition'
        if os.path.isdir(r'/Configs/ENB/'):
            self.conf_r = r'/Configs/ENB/'
        else:
            self.conf_r = r'../Configs/ENB/'

    def join(self):
        client = read_raw_sql_sat(db_con=connect_to_db(layer=self.schema_src), date=self.date, schema=self.schema_src,
                                t_name=self.src_client)

        disp = read_raw_sql_sat(db_con=connect_to_db(layer=self.schema_src), date=self.date, schema=self.schema_src,
                                t_name=self.src_disp)

        loan = read_raw_sql_sat(db_con=connect_to_db(layer=self.schema_src), date=self.date, schema=self.schema_src,
                                t_name=self.src_loan)


        acct = read_raw_sql_sat(db_con=connect_to_db(layer=self.schema_src), date=self.date, schema=self.schema_src,
                                t_name=self.src_acct)

        out_data = client.merge(disp, how='inner', left_on='client_id', right_on='client_id', suffixes=('', '_disp')).merge(acct,how='inner',left_on='account_id',right_on='account_id',suffixes=('','_acct')).merge(loan,how='inner',left_on='account_id',right_on='account_id',suffixes=('','_loan'))
        out_data = out_data[['loan_id','client_id']]
        out_data = out_data[out_data.notnull()]
        return out_data

    def mapping(self,data:pd.DataFrame):
        out_data = pd.DataFrame()
        out_data['darlehen_hk'] = data['loan_id'].apply(lambda x: hashlib.md5(x.encode()).hexdigest().upper())
        out_data['geschaeftspartner_hk'] = data['client_id'].apply(lambda x: hashlib.md5(x.encode()).hexdigest().upper())
        out_data['help_str'] = out_data.astype(str).agg(''.join, axis=1)
        out_data['gp_darlehen_hk'] = out_data['help_str'].apply(
            lambda x: hashlib.md5(x.encode()).hexdigest().upper())
        out_data.drop(inplace=True, columns='help_str')

        return out_data

    def writeToDB(self, data: pd.DataFrame):
        logging.info(colored('INFO: Entity ' + self.target, color='green'))
        con = connect_to_db(layer=self.schema_trg)

        loader = ILoader(date=self.date, loader_type='datavault',
                         loading_sat='l_s_gp_darlehen',
                         loading_entity=self.target,
                         target_connection=con,
                         schema=self.schema_trg)
        loader.load(data=data)

        logging.info('--- Beladung Ende ---\n')


konto = Link_GP_DARLEHEN('2018-12-31')
konto.writeToDB(konto.mapping(konto.join()))