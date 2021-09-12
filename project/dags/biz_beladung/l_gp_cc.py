import hashlib
import os
from datetime import datetime
import pandas as pd
import yaml
from termcolor import colored
import logging

from dwhutils.ILoader import ILoader
from dwhutils.TableReader import read_raw_sql_sat
from dwhutils.db_connection import connect_to_db


class Link_GP_CC:
    def __init__(self, date):
        self.date = date
        self.date_dt = datetime.strptime(date, '%Y-%m-%d')
        self.schema_trg = 'biz'
        self.target = 'gp_cc'
        self.schema_src = 'src'
        self.src_card = 'card'
        self.src_disp = 'disposition'
        self.load_domain = self.__class__.__name__.upper()
        self.src_client = 'client'
        if os.path.isdir(r'/Configs/ENB/'):
            self.conf_r = r'/Configs/ENB/'
        else:
            self.conf_r = r'../Configs/ENB/'

    def join(self):
        card = read_raw_sql_sat(db_con=connect_to_db(layer=self.schema_src), date=self.date, schema=self.schema_src,
                                t_name=self.src_card)

        disp = read_raw_sql_sat(db_con=connect_to_db(layer=self.schema_src), date=self.date, schema=self.schema_src,
                                t_name=self.src_disp)

        client = read_raw_sql_sat(db_con=connect_to_db(layer=self.schema_src), date=self.date, schema=self.schema_src,
                                  t_name=self.src_client)

        out_data = client.merge(disp, how='inner', left_on='client_id', right_on='client_id',
                                suffixes=('', '_disp')).merge(
            card, how='inner', left_on='disp_id', right_on='disp_id', suffixes=('', '_card'))

        out_data = out_data[['card_id', 'client_id']]
        return out_data

    def mapping(self, data: pd.DataFrame):
        out_data = pd.DataFrame()
        out_data['kreditkarte_hk'] = data['card_id'].apply(lambda x: hashlib.md5(x.encode()).hexdigest().upper())
        out_data['geschaeftspartner_hk'] = data['client_id'].apply(
            lambda x: hashlib.md5(x.encode()).hexdigest().upper())
        out_data['load_domain'] = self.load_domain

        return out_data

    def writeToDB(self, data: pd.DataFrame):
        logging.info(colored('INFO: Entity ' + self.target, color='green'))
        con = connect_to_db(layer=self.schema_trg)

        loader = ILoader(date=self.date, loader_type='datavault',
                         loading_sat='l_m_gp_cc',
                         loading_entity=self.target,
                         target_connection=con,
                         schema=self.schema_trg, build_hash_key=True, load_domain=self.load_domain)
        loader.load(data=data)

        logging.info('--- Beladung Ende ---\n')


link = Link_GP_CC('2018-12-31')
data = link.mapping(link.join())
link.writeToDB(data)
