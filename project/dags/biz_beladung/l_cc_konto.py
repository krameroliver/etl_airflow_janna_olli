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
except ImportError:
    from project.dags.utils.DataVaultLoader import DataVaultLoader
    from project.dags.utils.TableReader import read_raw_sql_sat
    from project.dags.utils.TechFields import add_technical_col
    from project.dags.utils.db_connection import connect_to_db


class Link_CC_Konto:
    def __init__(self, date):
        self.date = date
        self.date_dt = datetime.strptime(date, '%Y-%m-%d')
        self.schema_trg = 'biz'
        self.target = 'cc_konto'
        self.schema_src = 'src'
        self.src_card = 'card'
        self.src_trans = 'trans'
        self.src_disp = 'disposition'
        self.src_acct = 'acct'
        if os.path.isdir(r'/Configs/ENB/'):
            self.conf_r = r'/Configs/ENB/'
        else:
            self.conf_r = r'../Configs/ENB/'

    def join(self):
        card = read_raw_sql_sat(db_con=connect_to_db(layer=self.schema_src), date=self.date, schema=self.schema_src,
                                t_name=self.src_card)


        disp = read_raw_sql_sat(db_con=connect_to_db(layer=self.schema_src), date=self.date, schema=self.schema_src,
                                t_name=self.src_disp)

        acct = read_raw_sql_sat(db_con=connect_to_db(layer=self.schema_src), date=self.date, schema=self.schema_src,
                                t_name=self.src_acct)

        out_data = card.merge(disp, how='left', left_on='disp_id', right_on='disp_id', suffixes=('', '_disp')).merge(
            acct, how='left', left_on='account_id', right_on='account_id', suffixes=('', '_acct'))#.merge(trans,
                                                                                                  #       how='left',
                                                                                                  #       left_on='account_id',
                                                                                                  #       right_on='account_id',
                                                                                                  #       suffixes=(
                                                                                                  #       '', '_trans'))
        out_data = out_data[['card_id','account_id','user_type']]
        out_data = out_data[out_data.notnull()]
        return out_data


    def lkp_user_type(self,type:str):
        lkp = {
            'User':1,
            'Owner':0,
            '':99
        }
        return lkp[type]


    # def lkp_opperation(self,op:str):
    #     lkp = {
    #         'Credit in Cash':1,
    #         'Cash Withdrawal':2,
    #         'Remittance to Another Bank':3,
    #         'Collection from Another Bank':4,
    #         'Credit Card Withdrawal':5,
    #         '':99
    #
    #     }
    #     return lkp[op]

    def mapping(self,data:pd.DataFrame):
        out_data = pd.DataFrame()
        out_data['kreditkarte_hk'] = data['card_id'].apply(lambda x: hashlib.md5(x.encode()).hexdigest().upper())
        out_data['konto_hk'] = data['account_id'].apply(lambda x: hashlib.md5(x.encode()).hexdigest().upper())
        out_data['rolle'] = data['user_type'].apply(lambda x: self.lkp_user_type(x))

        return out_data

    def writeToDB(self, data: pd.DataFrame):
        logging.info(colored('INFO: Entity ' + self.target, color='green'))
        con = connect_to_db(layer=self.schema_trg)
        sat_data = add_technical_col(data=data, t_name="l_m_cc_konto", date=self.date, entity_name=self.target)
        with open(self.conf_r + self.target + '.yaml') as file:
            documents = yaml.full_load(file)
        hub_target_fields = documents[self.target]['tables']['l_cc_konto']['fields']
        hub_target_fields.append(documents[self.target]['tables']['l_cc_konto']['hash_key'])
        hub_res_data = pd.DataFrame(columns=hub_target_fields)
        hub_res_data[hub_target_fields] = sat_data[hub_target_fields]

        print('SAT')

        dv_sat = DataVaultLoader(data=sat_data, db_con=con, entity_name=self.target, t_name='l_m_cc_konto',
                                 date=self.date, schema=self.schema_trg)

        dv_sat.load

        print('HUB')

        dv_hub = DataVaultLoader(data=hub_res_data, db_con=con, entity_name=self.target, t_name='l_cc_konto',
                                 date=self.date, schema=self.schema_trg)

        dv_hub.load
        logging.info('--- Beladung Ende ---\n')


