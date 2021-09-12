# Import Pakete
# import utils as u
# import utils as u
import hashlib
import os
from datetime import datetime

import pandas as pd

from dateutil.relativedelta import relativedelta
from dwhutils.ILoader import ILoader
from dwhutils.TableReader import read_raw_sql_sat
from dwhutils.db_connection import connect_to_db




class Ko:
    def __init__(self, date):
        self.date = date
        self.date_dt = datetime.strptime(date, '%Y-%m-%d')
        self.schema_trg = 'biz'
        self.target = 'konto'
        self.schema_src = 'src'
        self.src_acct = 'acct'
        self.load_domain = self.__class__.__name__.upper()
        self.src_trans = 'trans'
        self.conf_r = os.getenv('ENTITY_CONFIGS')

    def join(self):
        account = read_raw_sql_sat(db_con=connect_to_db(layer=self.schema_src), date=self.date, schema=self.schema_src,
                                   t_name=self.src_acct)
        trans = read_raw_sql_sat(db_con=connect_to_db(layer=self.schema_src), date=self.date, schema=self.schema_src,
                                 t_name=self.src_trans)
        trans = trans[['account_id', 'balance', 'fulldatewithtime']]

        # trans['fulldatewithtime']=trans['fulldatewithtime'].apply(lambda x: self.timestamp(x))

        max_ids = trans.groupby(['account_id'])['fulldatewithtime'].transform(max) == trans['fulldatewithtime']
        trans = trans[max_ids]

        # print(trans.head)

        data = account.merge(trans, how='inner', left_on=['account_id'], right_on=['account_id'])

        return data

    def timestamp(self, ts):
        ts = ts.replace('T', ' ')
        element = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
        timestamp = datetime.timestamp(element)
        return (timestamp)

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
        out_data['load_domain'] = self.load_domain
        out_data['konto_hk'] = data['account_id'].apply(lambda x: hashlib.md5(x.encode()).hexdigest().upper())

        return out_data

    def writeToDB(self, data: pd.DataFrame):
        con = connect_to_db(layer=self.schema_trg)
        loader = ILoader(date=self.date,loader_type='datavault', loading_sat='s_konto', loading_entity=self.target,
                         target_connection=con,
                         schema=self.schema_trg,build_hash_key=False,load_domain=self.load_domain)
        loader.load(data=data)


        print('--- Beladung Ende ---\n')

konto = Ko('2018-12-31')
konto.writeToDB(konto.mapping(konto.join()))
