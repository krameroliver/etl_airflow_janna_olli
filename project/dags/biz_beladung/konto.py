# Import Pakete
# import utils as u
# import utils as u

from datetime import datetime

import pandas as pd
import yaml
from dateutil.relativedelta import relativedelta
from termcolor2 import colored
try:
    from utils.DataVaultLoader import DataVaultLoader
    from utils.TableReader import read_raw_sql_sat
    from utils.TechFields import add_technical_col
    from utils.db_connection import connect_to_db
except ImportError:
    from project.dags.utils.TableReader import read_raw_sql_sat
    from project.dags.utils.db_connection import connect_to_db
    from project.dags.utils.TechFields import add_technical_col
    from project.dags.utils.DataVaultLoader import DataVaultLoader

class Ko:
    def __init__(self, date):
        self.date = date
        self.date_dt = datetime.strptime(date, '%Y-%m-%d')
        self.schema_trg = 'biz'
        self.target = 'konto'
        self.schema_src = 'src'
        self.src_acct = 'acct'


    def join(self):
        account = read_raw_sql_sat(db_con=connect_to_db(layer=self.schema_src), date=self.date, schema=self.schema_src,
                                  t_name=self.src_acct)

        data = account

        return data

    def lkp_frequenz(self, freq: str):
        frequenz = {
            'Monthly Issuance': 1,
            'Issuance After Transaction': 0,
            'Weekly Issuance': 2}

        return frequenz[freq]

    def mapping(self, data: pd.DataFrame):

        out_data = pd.DataFrame()
        out_data['kontonummer'] = data['account_id']
        out_data['frequenz'] = data['frequency'].apply(lambda x: self.lkp_frequenz(x))
        out_data['wertstellungstag'] = data['parseddate']

        return out_data


    def writeToDB(self, data: pd.DataFrame):
        print(colored('INFO: Entity ' + self.target, color='green'))
        con = connect_to_db(layer=self.schema_trg)
        sat_data = add_technical_col(data=data, t_name='s_konto', date=self.date, entity_name=self.target)
        with open(r'../Configs/ENB/' + self.target + '.yaml') as file:
            documents = yaml.full_load(file)
        hub_target_fields = documents[self.target]['tables']['h_' + self.target]['fields']
        hub_target_fields.append(documents[self.target]['tables']['h_' + self.target]['hash_key'])
        hub_res_data = pd.DataFrame(columns=hub_target_fields)
        hub_res_data[hub_target_fields] = sat_data[hub_target_fields]
        dv_sat = DataVaultLoader(data=sat_data, db_con=con, entity_name=self.target, t_name='s_' + self.target,
                                 date=self.date, schema=self.schema_trg)
        dv_hub = DataVaultLoader(data=hub_res_data, db_con=con, entity_name=self.target, t_name='h_' + self.target,
                                 date=self.date, schema=self.schema_trg)
        dv_sat.load
        dv_hub.load

        print('--- Beladung Ende ---\n')

