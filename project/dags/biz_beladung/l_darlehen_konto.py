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


class Link_DARLEHEN_KONTO:
    def __init__(self, date):
        self.date = date
        self.date_dt = datetime.strptime(date, '%Y-%m-%d')
        self.schema_trg = 'biz'
        self.target = 'darlehen_konto'
        self.schema_src = 'src'
        self.src_loan = 'loan'
        self.load_domain = self.__class__.__name__.upper()
        self.src_acct = 'acct'
        if os.path.isdir(r'/Configs/ENB/'):
            self.conf_r = r'/Configs/ENB/'
        else:
            self.conf_r = r'../Configs/ENB/'

    def join(self):
        loan = read_raw_sql_sat(db_con=connect_to_db(layer=self.schema_src), date=self.date, schema=self.schema_src,
                                t_name=self.src_loan)

        acct = read_raw_sql_sat(db_con=connect_to_db(layer=self.schema_src), date=self.date, schema=self.schema_src,
                                t_name=self.src_acct)

        out_data = loan.merge(acct, how='left', left_on='account_id', right_on='account_id', suffixes=('', '_acct'))
        # out_data = out_data[['loan_id','account_id']]
        # out_data = out_data[out_data.notnull()]
        return out_data

    def mapping(self, data: pd.DataFrame):
        out_data = pd.DataFrame()
        out_data['darlehen_hk'] = data['loan_id'].apply(lambda x: hashlib.md5(x.encode()).hexdigest().upper())
        out_data['konto_hk'] = data['account_id'].apply(lambda x: hashlib.md5(x.encode()).hexdigest().upper())
        out_data['help_str'] = out_data.astype(str).agg(''.join, axis=1)
        out_data['darlehen_konto_hk'] = out_data['help_str'].apply(lambda x: hashlib.md5(x.encode()).hexdigest().upper())
        out_data['load_domain'] = self.load_domain
        out_data.drop(inplace=True, columns='help_str')

        return out_data

    def writeToDB(self, data: pd.DataFrame):
        logging.info(colored('INFO: Entity ' + self.target, color='green'))
        con = connect_to_db(layer=self.schema_trg)
        loader = ILoader(date=self.date, loader_type='datavault',
                         loading_sat='l_s_darlehen_konto',
                         loading_entity=self.target,
                         target_connection=con,
                         schema=self.schema_trg,build_hash_key=True,load_domain=self.load_domain)
        loader.load(data=data)

        logging.info('--- Beladung Ende ---\n')


link = Link_DARLEHEN_KONTO('2018-12-31')
data = link.mapping(link.join())
link.writeToDB(data)
