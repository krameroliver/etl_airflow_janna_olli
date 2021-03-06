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
    from utils.lookup import get_lkp_value
except ImportError:
    from project.dags.utils.DataVaultLoader import DataVaultLoader
    from project.dags.utils.TableReader import read_raw_sql_sat
    from project.dags.utils.TechFields import add_technical_col
    from project.dags.utils.db_connection import connect_to_db
    from project.dags.utils.lookup import get_lkp_value


class Link_GP_KONTO:
    def __init__(self, date):
        self.date = date
        self.date_dt = datetime.strptime(date, '%Y-%m-%d')
        self.schema_trg = 'biz'
        self.target = 'gp_konto'
        self.schema_src = 'src'
        self.src_acct = 'acct'
        self.src_disp = 'disposition'
        self.src_client = 'client'
        if os.path.isdir(r'/Configs/ENB/'):
            self.conf_r = r'/Configs/ENB/'
        else:
            self.conf_r = r'../Configs/ENB/'

    def join(self):
        acct = read_raw_sql_sat(db_con=connect_to_db(layer=self.schema_src), date=self.date, schema=self.schema_src,
                                t_name=self.src_acct)

        disp = read_raw_sql_sat(db_con=connect_to_db(layer=self.schema_src), date=self.date, schema=self.schema_src,
                                t_name=self.src_disp)

        client = read_raw_sql_sat(db_con=connect_to_db(layer=self.schema_src), date=self.date, schema=self.schema_src,
                                t_name=self.src_client)

        out_data = client.merge(disp, how='inner', left_on='client_id', right_on='client_id', suffixes=('', '_disp')).merge(
            acct, how='inner', left_on='account_id', right_on='account_id', suffixes=('', '_card'))


        out_data = out_data[['account_id','client_id','user_type']]

        return out_data




    def mapping(self,data:pd.DataFrame):
        lkp_cf_opperation = get_lkp_value(lkp_name='USER_TYPE')

        out_data = pd.DataFrame()
        out_data['konto_hk'] = data['account_id'].apply(lambda x: hashlib.md5(x.encode()).hexdigest().upper())
        out_data['geschaeftspartner_hk'] = data['client_id'].apply(lambda x: hashlib.md5(x.encode()).hexdigest().upper())
        out_data['gp_rolle'] = data['user_type'].apply(lambda x: lkp_cf_opperation[x])

        return out_data

    def writeToDB(self, data: pd.DataFrame):
        logging.info(colored('INFO: Entity ' + self.target, color='green'))
        con = connect_to_db(layer=self.schema_trg)
        sat_data = add_technical_col(data=data, t_name="l_m_gp_konto", date=self.date, entity_name=self.target)
        with open(self.conf_r + self.target + '.yaml') as file:
            documents = yaml.full_load(file)
        hub_target_fields = documents[self.target]['tables']['l_gp_konto']['fields']
        hub_target_fields.append(documents[self.target]['tables']['l_gp_konto']['hash_key'])
        hub_res_data = pd.DataFrame(columns=hub_target_fields)
        hub_res_data[hub_target_fields] = sat_data[hub_target_fields]

        dv_sat = DataVaultLoader(data=sat_data, db_con=con, entity_name=self.target, t_name='l_m_gp_konto',
                                 date=self.date, schema=self.schema_trg)
        dv_sat.load
        del(dv_sat)
        del(sat_data)
        dv_hub = DataVaultLoader(data=hub_res_data, db_con=con, entity_name=self.target, t_name='l_gp_konto',
                                 date=self.date, schema=self.schema_trg)

        dv_hub.load
        logging.info('--- Beladung Ende ---\n')


