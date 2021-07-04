import hashlib
from datetime import datetime
import pandas as pd
import logging

try:
    from utils.db_connection import connect_to_db
except ImportError:
    from project.dags.utils.db_connection import connect_to_db


class lookup:
    def __init__(self,lkp_name:str,lkp_data:dict):
        self.lkp_name = 'lkp_'+lkp_name
        self.lkp_data = pd.DataFrame(data=lkp_data)

    def __repr__(self):
        print('{lkp_name}:')
        print(self.lkp_data)

    @property
    def get_lkp(self):
        return (self.lkp_name,self.lkp_data)




class LookUps:
    def __init__(self):
        self.schema_trg = 'biz'
        self.target = 'lookup'

    @property
    def insert_into_db(self):
        con = connect_to_db(self.schema_trg)

        logging.info('WRITE STATUS')
        lkp = lookup('STATUS',{'auspraegung':['A','B','C','D'],'ID':[1,2,3,4]}).get_lkp
        lkp[1].to_sql(schema=self.schema_trg,con=con,if_exists='replace',name=lkp[0],index=False)
        logging.info('WRITE USER_TYPE')
        lkp = lookup('USER_TYPE', {'auspraegung': ['User', 'Owner', ''], 'ID': [1, 0, 99]}).get_lkp
        lkp[1].to_sql(schema=self.schema_trg, con=con, if_exists='replace', name=lkp[0], index=False)
        logging.info('WRITE DURCHSCHNITTSALTER')
        lkp = lookup('DURCHSCHNITTSALTER', {'auspraegung': ['Female', 'Male', 'Div'], 'ID': [84,79, 82]}).get_lkp
        lkp[1].to_sql(schema=self.schema_trg, con=con, if_exists='replace', name=lkp[0], index=False)
        logging.info('WRITE SEX')
        lkp = lookup('SEX', {'auspraegung': ['Female', 'Male', 'Div'], 'ID': [0, 1, 2]}).get_lkp
        lkp[1].to_sql(schema=self.schema_trg, con=con, if_exists='replace', name=lkp[0], index=False)
        logging.info('WRITE ANREDE')
        lkp = lookup('ANREDE', {'auspraegung': ['Female', 'Male', 'Div'], 'ID': ['Frau', 'Herr', 'Mensch']}).get_lkp
        lkp[1].to_sql(schema=self.schema_trg, con=con, if_exists='replace', name=lkp[0], index=False)
        logging.info('WRITE CARTTYPE')
        lkp = lookup('CARTTYPE', {'auspraegung': ['VISA Signature', 'VISA Standard', 'VISA Infinite'], 'ID': [0,1,2]}).get_lkp
        lkp[1].to_sql(schema=self.schema_trg, con=con, if_exists='replace', name=lkp[0], index=False)



lkp = LookUps()
lkp.insert_into_db
