# Import Pakete
# import utils as u
# import utils as u
import os
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
        self.src_trans = 'trans'
        if os.path.isdir(r'/Configs/ENB/'):
            self.conf_r = r'/Configs/ENB/'
        else:
            self.conf_r = r'../Configs/ENB/'


    def join(self):
        account = read_raw_sql_sat(db_con=connect_to_db(layer=self.schema_src), date=self.date, schema=self.schema_src,
                                  t_name=self.src_acct)
        trans = read_raw_sql_sat(db_con=connect_to_db(layer=self.schema_src), date=self.date, schema=self.schema_src,
                                  t_name=self.src_trans)
        trans=trans[['account_id','balance','fulldatewithtime']]

        #trans['fulldatewithtime']=trans['fulldatewithtime'].apply(lambda x: self.timestamp(x))

        max_ids=trans.groupby(['account_id'])['fulldatewithtime'].transform(max)==trans['fulldatewithtime']
        trans=trans[max_ids]

        #print(trans.head)


        data = account.merge(trans,  how='inner', left_on=['account_id'], right_on=['account_id'])

        return data

    def timestamp(self, ts):
        ts=ts.replace('T',' ')
        element = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
        timestamp = datetime.timestamp(element)
        return(timestamp)

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
        out_data['kontoerstellung_dt'] = data['parseddate']
        out_data['wertstellungszeitpunkt'] = data['fulldatewithtime']
        out_data['kontostand'] = data['balance']

        return out_data


    def writeToDB(self, data: pd.DataFrame):
        print(colored('INFO: Entity ' + self.target, color='green'))
        con = connect_to_db(layer=self.schema_trg)
        sat_data = add_technical_col(data=data, t_name='s_konto', date=self.date, entity_name=self.target)
        with open(self.conf_r + self.target + '.yaml') as file:
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


#konto=Ko('2018-12-31')
#join=konto.join()
#map=konto.mapping(join)
#konto.writeToDB(map)


