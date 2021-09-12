import hashlib
import os
from datetime import datetime
import pandas as pd
import yaml
from dotenv import load_dotenv
from termcolor import colored
import logging

from dwhutils.ILoader import ILoader
from dwhutils.TableReader import read_raw_sql_sat
from dwhutils.db_connection import connect_to_db

class LinkCcKonto:
    def __init__(self, date):
        self.date = date
        self.date_dt = datetime.strptime(date, '%Y-%m-%d')
        self.schema_trg = 'biz'
        self.target = 'cc_konto'
        self.schema_src = 'src'
        self.src_card = 'card'
        self.load_domain = self.__class__.__name__.upper()
        self.src_trans = 'trans'
        self.src_disp = 'disposition'
        self.src_acct = 'acct'
        load_dotenv()
        self.conf_r = os.getenv('ENTITY_CONFIGS')

    @property
    def join(self):
        card = read_raw_sql_sat(db_con=connect_to_db(layer=self.schema_src), date=self.date, schema=self.schema_src,
                                t_name=self.src_card)

        disp = read_raw_sql_sat(db_con=connect_to_db(layer=self.schema_src), date=self.date, schema=self.schema_src,
                                t_name=self.src_disp)

        acct = read_raw_sql_sat(db_con=connect_to_db(layer=self.schema_src), date=self.date, schema=self.schema_src,
                                t_name=self.src_acct)

        out_data = card.merge(disp, how='left', left_on='disp_id', right_on='disp_id', suffixes=('', '_disp')).merge(
            acct, how='left', left_on='account_id', right_on='account_id', suffixes=('', '_acct'))  # .merge(trans,
        #       how='left',
        #       left_on='account_id',
        #       right_on='account_id',
        #       suffixes=(
        #       '', '_trans'))
        out_data = out_data[['card_id', 'account_id', 'user_type']]
        out_data = out_data[out_data.notnull()]
        return out_data

    def lkp_user_type(self, type: str):
        lkp = {
            'User': 1,
            'Owner': 0,
            '': 99
        }
        return lkp[type]

    def mapping(self, data: pd.DataFrame):
        out_data = pd.DataFrame()
        out_data['kreditkarte_hk'] = data['card_id'].apply(lambda x: hashlib.md5(x.encode()).hexdigest().upper())
        out_data['konto_hk'] = data['account_id'].apply(lambda x: hashlib.md5(x.encode()).hexdigest().upper())
        out_data['rolle'] = data['user_type'].apply(lambda x: self.lkp_user_type(x))
        out_data['load_domain'] = self.load_domain

        return out_data

    def writeToDB(self, data: pd.DataFrame):
        logging.info(colored('INFO: Entity ' + self.target, color='green'))

        con = connect_to_db(layer=self.schema_trg)

        loader = ILoader(date=self.date, loader_type='datavault',
                         loading_sat='l_m_cc_konto',
                         loading_entity=self.target,
                         target_connection=con,
                         schema=self.schema_trg, build_hash_key=True, load_domain=self.load_domain)
        loader.load(data=data)

        logging.info('--- Beladung Ende ---\n')


konto = LinkCcKonto('2018-12-31')
konto.writeToDB(konto.mapping(konto.join))
