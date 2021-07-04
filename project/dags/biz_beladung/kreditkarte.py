from datetime import datetime

import pandas as pd
import yaml
from termcolor2 import colored
from utils.DataVaultLoader import DataVaultLoader
# try:
from utils.TableReader import read_raw_sql_sat
from utils.TechFields import add_technical_col
from utils.db_connection import connect_to_db


class Kreditkarte:
    def __init__(self, date):
        self.date = date
        self.date_dt = datetime.strptime(date, '%Y-%m-%d')
        self.schema_trg = 'biz'
        self.target = 'kreditkarte'
        self.schema_src = 'src'
        self.src_card = 'card'

    def join(self):
        card = read_raw_sql_sat(db_con=connect_to_db(layer=self.schema_src), date=self.date, schema=self.schema_src,
                                t_name=self.src_card)
        with open(r'/Configs/ENB/' + self.src_card + '.yaml') as file:
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
        with open(r'/Configs/ENB/' + self.target + '.yaml') as file:
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
        sat_data = add_technical_col(data=data, t_name="s_kreditkarte", date=self.date, entity_name=self.target)
        with open(r'/Configs/ENB/' + self.target + '.yaml') as file:
            documents = yaml.full_load(file)
        hub_target_fields = documents[self.target]['tables']['h_' + self.target]['fields']
        hub_res_data = pd.DataFrame(columns=hub_target_fields)
        hub_res_data[hub_target_fields] = sat_data[hub_target_fields]

        dv_sat = DataVaultLoader(data=sat_data, db_con=con, entity_name=self.target, t_name="s_kreditkarte",
                                 date=self.date, schema=self.schema_trg)
        dv_hub = DataVaultLoader(data=hub_res_data, db_con=con, entity_name=self.target, t_name="h_kreditkarte",
                                 date=self.date, schema=self.schema_trg)
        dv_sat.load
        dv_hub.load
        print('--- Beladung Ende ---\n')
