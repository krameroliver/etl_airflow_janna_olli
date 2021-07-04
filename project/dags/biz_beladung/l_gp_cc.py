import hashlib
from datetime import datetime
import pandas as pd
import yaml
from termcolor import colored
import logging

from utils.DataVaultLoader import DataVaultLoader
from utils.TableReader import read_raw_sql_sat
from utils.TechFields import add_technical_col
from utils.db_connection import connect_to_db


class Link_GP_CC:
    def __init__(self, date):
        self.date = date
        self.date_dt = datetime.strptime(date, '%Y-%m-%d')
        self.schema_trg = 'biz'
        self.target = 'gp_cc'
        self.schema_src = 'src'
        self.src_card = 'card'
        self.src_disp = 'disposition'
        self.src_client = 'client'

    def join(self):
        card = read_raw_sql_sat(db_con=connect_to_db(layer=self.schema_src), date=self.date, schema=self.schema_src,
                                t_name=self.src_card)

        disp = read_raw_sql_sat(db_con=connect_to_db(layer=self.schema_src), date=self.date, schema=self.schema_src,
                                t_name=self.src_disp)

        client = read_raw_sql_sat(db_con=connect_to_db(layer=self.schema_src), date=self.date, schema=self.schema_src,
                                t_name=self.src_client)

        out_data = client.merge(disp, how='inner', left_on='client_id', right_on='client_id', suffixes=('', '_disp')).merge(
            card, how='inner', left_on='disp_id', right_on='disp_id', suffixes=('', '_card'))


        out_data = out_data[['card_id','client_id']]

        #out_data = out_data[out_data['card_id','client_id']!='NaN']
        print(out_data)
        return out_data




    def mapping(self,data:pd.DataFrame):
        out_data = pd.DataFrame()
        out_data['kreditkarte_hk'] = data['card_id'].apply(lambda x: hashlib.md5(x.encode()).hexdigest().upper())
        out_data['geschaeftspartner_hk'] = data['client_id'].apply(lambda x: hashlib.md5(x.encode()).hexdigest().upper())


        return out_data

    def writeToDB(self, data: pd.DataFrame):
        logging.info(colored('INFO: Entity ' + self.target, color='green'))
        con = connect_to_db(layer=self.schema_trg)
        sat_data = add_technical_col(data=data, t_name="l_s_gp_cc", date=self.date, entity_name=self.target)
        with open(r'/Configs/ENB/' + self.target + '.yaml') as file:
            documents = yaml.full_load(file)
        hub_target_fields = documents[self.target]['tables']['l_gp_cc']['fields']
        hub_target_fields.append(documents[self.target]['tables']['l_gp_cc']['hash_key'])
        hub_res_data = pd.DataFrame(columns=hub_target_fields)
        hub_res_data[hub_target_fields] = sat_data[hub_target_fields]

        dv_sat = DataVaultLoader(data=sat_data, db_con=con, entity_name=self.target, t_name='l_s_gp_cc',
                                 date=self.date, schema=self.schema_trg)
        dv_sat.load
        del(dv_sat)
        del(sat_data)
        dv_hub = DataVaultLoader(data=hub_res_data, db_con=con, entity_name=self.target, t_name='l_gp_cc',
                                 date=self.date, schema=self.schema_trg)

        dv_hub.load
        logging.info('--- Beladung Ende ---\n')


