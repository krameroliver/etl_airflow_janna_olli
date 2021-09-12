import logging

import pandas as pd

from dwhutils.ILoader import ILoader
from dwhutils.TableReader import read_raw_sql_sat
from dwhutils.db_connection import connect_to_db


class lookup:
    def __init__(self, lkp_name: str, lkp_data: dict):
        self.lkp_name = lkp_name
        self.lkp_data = pd.DataFrame(data=lkp_data)

    def __repr__(self):
        print('{lkp_name}:')
        print(self.lkp_data)

    @property
    def get_lkp(self):
        return (self.lkp_name, self.lkp_data)




class LookUps:
    def __init__(self):
        self.schema_trg = 'biz'
        self.target = 'lookup'

    @property
    def insert_into_db(self):
        con = connect_to_db(self.schema_trg)



        lkp = lookup('payment_type'.upper(), {
            'auspraegung': ['Household Payment', 'Loan Payment', 'Leasing Payment',
                            'Insurance Payment', 'Payment on Statement','Interest Credited','Household','Old Age Pension','Sanction Interest', ''],
            'ID': [0, 1, 2, 3,4,5,6,7,8, 99]}).get_lkp
        lkp[1].to_sql(schema=self.schema_trg, con=con, if_exists='replace', name=lkp[0], index=False)

