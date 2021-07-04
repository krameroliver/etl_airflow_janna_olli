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
except ImportError:
    from project.dags.utils.DataVaultLoader import DataVaultLoader
    from project.dags.utils.TableReader import read_raw_sql_sat
    from project.dags.utils.TechFields import add_technical_col
    from project.dags.utils.db_connection import connect_to_db


class Transaktion:
    def __init__(self, date):
        self.date = date
        self.date_dt = datetime.strptime(date, '%Y-%m-%d')
        self.schema_trg = 'biz'
        self.target = 'transaktion'
        self.schema_src = 'src'
        self.src_trans = 'trans'
        if os.path.isdir(r'/Configs/ENB/'):
            self.conf_r = r'/Configs/ENB/'
        else:
            self.conf_r = r'../Configs/ENB/'

    def join(self):
        trans = read_raw_sql_sat(db_con=connect_to_db(layer=self.schema_src), date=self.date, schema=self.schema_src,
                                 t_name=self.src_trans)
        with open(self.conf_r + self.src_trans + '.yaml') as file:
            documents = yaml.full_load(file)
        field_list = documents[self.src_trans]['tables'][self.src_trans]['fields']
        trans = trans[field_list]
        return trans

    def mapping(self, data: pd.DataFrame):
        with open(self.conf_r + self.target + '.yaml') as file:
            documents = yaml.full_load(file)
        sat_target_fields = documents[self.target]['tables']['s_' + self.target]['fields']
        sat_res_data = pd.DataFrame(columns=sat_target_fields)

        sat_res_data['transaktions_id'] = data['trans_id']
        sat_res_data['ausfuehrungsdatum'] = data['fulldate'].apply(lambda x: datetime.strptime(x,'%d.%m.%Y').date())
        sat_res_data['betrag'] = data['amount'].apply(lambda x: float(x.replace(',','.')))
        sat_res_data['loeschung'] = 10
        sat_res_data['buchungsart'] = 1
        return sat_res_data

    def writeToDB(self, data: pd.DataFrame):
        print(colored('INFO: Entity ' + self.target, color='green'))
        con = connect_to_db(layer=self.schema_trg)
        sat_data = add_technical_col(data=data, t_name="s_transaktion", date=self.date, entity_name=self.target)
        with open(self.conf_r + self.target + '.yaml') as file:
            documents = yaml.full_load(file)
        hub_target_fields = documents[self.target]['tables']['h_' + self.target]['fields']
        hub_res_data = pd.DataFrame(columns=hub_target_fields)
        hub_res_data[hub_target_fields] = sat_data[hub_target_fields]

        dv_sat = DataVaultLoader(data=sat_data, db_con=con, entity_name=self.target, t_name='s_transaktion',
                                 date=self.date, schema=self.schema_trg)
        dv_hub = DataVaultLoader(data=hub_res_data, db_con=con, entity_name=self.target, t_name='h_transaktion',
                                 date=self.date, schema=self.schema_trg)
        dv_sat.load
        dv_hub.load
        print('--- Beladung Ende ---\n')
