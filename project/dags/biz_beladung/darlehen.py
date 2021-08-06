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
    from utils.ILoader import ILoader
except ImportError:
    from project.dags.utils.DataVaultLoader import DataVaultLoader
    from project.dags.utils.TableReader import read_raw_sql_sat
    from project.dags.utils.TechFields import add_technical_col
    from project.dags.utils.db_connection import connect_to_db
    from project.dags.utils.ILoader import ILoader


class Darlehen:
    def __init__(self, date):
        self.date = date
        self.date_dt = datetime.strptime(date, '%Y-%m-%d')
        self.schema_trg = 'biz'
        self.target = 'darlehen'
        self.schema_src = 'src'
        self.src_loan = 'loan'
        if os.path.isdir(r'/Configs/ENB/'):
            self.conf_r = r'/Configs/ENB/'
        else:
            self.conf_r = r'../Configs/ENB/'

    def join(self):
        loan = read_raw_sql_sat(db_con=connect_to_db(layer=self.schema_src), date=self.date, schema=self.schema_src,
                                t_name=self.src_loan)
        with open(self.conf_r + self.src_loan + '.yaml') as file:
            documents = yaml.full_load(file)
        field_list = documents[self.src_loan]['tables'][self.src_loan]['fields']
        loan = loan[field_list]
        return loan

    def lkp_status(self, stat: str):
        lkp = {
            'A': 1,
            'B': 2,
            'C': 3,
            'D': 4
        }
        return lkp[stat.upper()]

    def lkp_verwendungszweck(self, zweck: str):
        lkp = {
            'CAR': 1,
            'HOME_IMPROVEMENT': 2,
            'HOME': 3,
            'DEBT_CONSOLIDATION': 4

        }
        return lkp[zweck.upper()]

    def mapping(self, data: pd.DataFrame):
        with open(self.conf_r + self.target + '.yaml') as file:
            documents = yaml.full_load(file)
        sat_target_fields = documents[self.target]['tables']['s_' + self.target]['fields']
        sat_res_data = pd.DataFrame(columns=sat_target_fields)

        sat_res_data['kontonummer'] = data['account_id']
        sat_res_data['nominal'] = data['amount']
        sat_res_data['startdatum'] = data['fulldate']
        sat_res_data['enddatum'] = '2262-04-11'
        sat_res_data['status'] = data['status'].apply(lambda x: self.lkp_status(x))
        sat_res_data['tilgung'] = data['payments']
        sat_res_data['futurecashflow'] = data['duration']
        sat_res_data['verwendungszweck'] = data['purpose'].apply(lambda x: self.lkp_verwendungszweck(x))
        sat_res_data['loeschung'] = 10
        sat_res_data[documents[self.target]['tables']['s_' + self.target]['hash_key']] = data['loan_id'].apply(lambda x: hashlib.md5(x.encode()).hexdigest().upper())
        return sat_res_data

    def writeToDB(self, data: pd.DataFrame):
        print(colored('INFO: Entity ' + self.target, color='green'))


        con = connect_to_db(layer=self.schema_trg)
        loader = ILoader(date=self.date, loader_type='datavault', loading_sat='s_darlehen', loading_entity=self.target,
                         target_connection=con,
                         schema=self.schema_trg)
        loader.load(data=data)
        print('--- Beladung Ende ---\n')


entity = Darlehen(date='2018-12-31')
entity.writeToDB(entity.mapping(entity.join()))