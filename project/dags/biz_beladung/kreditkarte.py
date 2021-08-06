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


class Kreditkarte:
    def __init__(self, date):
        self.date = date
        self.date_dt = datetime.strptime(date, '%Y-%m-%d')
        self.schema_trg = 'biz'
        self.target = 'kreditkarte'
        self.schema_src = 'src'
        self.src_card = 'card'
        if os.path.isdir(r'/Configs/ENB/'):
            self.conf_r = r'/Configs/ENB/'
        else:
            self.conf_r = r'../Configs/ENB/'

    def join(self):
        card = read_raw_sql_sat(db_con=connect_to_db(layer=self.schema_src), date=self.date, schema=self.schema_src,
                                t_name=self.src_card)
        with open(self.conf_r  + self.src_card + '.yaml') as file:
            documents = yaml.full_load(file)
        field_list = documents[self.src_card]['tables'][self.src_card]['fields']
        card = card[field_list]
        return card

    def lkp_cardtype(self, type: str):
        lkp = {
            'VISA Signature': 1,
            'VISA Standard': 2,
            'VISA Infinite': 3
        }
        return lkp[type]

    def mapping(self, data: pd.DataFrame):
        with open(self.conf_r  + self.target + '.yaml') as file:
            documents = yaml.full_load(file)
        sat_target_fields = documents[self.target]['tables']['s_' + self.target]['fields']
        sat_res_data = pd.DataFrame(columns=sat_target_fields)

        sat_res_data['kartennummer'] = data['card_id']
        sat_res_data['beginndatum'] = data['fulldate']
        sat_res_data['kartentyp'] = data['card_type'].apply(lambda x: self.lkp_cardtype(x))
        sat_res_data['loeschung'] = 10

        return sat_res_data

    def writeToDB(self, data: pd.DataFrame):
        print(colored('INFO: Entity ' + self.target, color='green'))
        con = connect_to_db(layer=self.schema_trg)

        loader = ILoader(date=self.date, loader_type='datavault',
                         loading_sat='s_kreditkarte',
                         loading_entity=self.target,
                         target_connection=con,
                         schema=self.schema_trg)
        loader.load(data=data)


        print('--- Beladung Ende ---\n')

