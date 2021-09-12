import warnings
from datetime import datetime

import pandas as pd
import yaml
from dwhutils.Logger import logger

warnings.filterwarnings("ignore")

from dwhutils.TableReader import read_raw_sql_sat
from dwhutils.db_connection import connect_to_db
from dwhutils.ILoader import ILoader
import os


class Darlehen:
    def __init__(self, date):
        self.date = date
        self.date_dt = datetime.strptime(date, '%Y-%m-%d')
        self.schema_trg = 'biz'
        self.target = 'darlehen'
        self.cli_log = True
        self.file_log = False
        self.schema_src = 'src'
        self.src_loan = 'loan'
        self.load_domain = self.__class__.__name__
        self.conf_r = os.getenv('ENTITY_CONFIGS')
        self.doc_src = os.path.join(self.conf_r, self.src_loan + '.yaml')
        self.doc_trg = os.path.join(self.conf_r, self.target + '.yaml')

    def join(self):
        con = connect_to_db(layer=self.schema_src)
        print(con)
        _loan = read_raw_sql_sat(db_con=con, date=self.date, schema=self.schema_src,
                                 t_name=self.src_loan)
        with open(self.doc_src) as file:
            documents = yaml.full_load(file)
        field_list = documents[self.src_loan]['tables'][self.src_loan]['fields']
        field_list.append(documents[self.src_loan]['tables'][self.src_loan]['hash_key'])
        loan = _loan[field_list]

        return loan

    def lkp_status(self, stat: str):
        lkp = {
            'A': 1,
            'B': 2,
            'C': 3,
            'D': 4
        }
        return lkp[stat.upper()]

    def lkp_verwendungszweck(self, zweck: str):
        lkp = {
            'CAR': 1,
            'HOME_IMPROVEMENT': 2,
            'HOME': 3,
            'DEBT_CONSOLIDATION': 4

        }
        return lkp[zweck.upper()]

    def mapping(self, data: pd.DataFrame):
        with open(self.doc_trg) as file:
            documents = yaml.full_load(file)
        sat_target_fields = documents[self.target]['tables']['s_' + self.target]['fields']
        sat_res_data = pd.DataFrame(columns=sat_target_fields)
        with open(self.doc_trg) as file:
            _documents = yaml.full_load(file)
        print("mapping")
        sat_res_data['darlehensnummer'] = data['loan_id']
        sat_res_data['nominal'] = data['amount']
        sat_res_data['startdatum'] = data['fulldate']
        sat_res_data['enddatum'] = '2262-04-11'
        sat_res_data['status'] = data['status'].apply(lambda x: self.lkp_status(x))
        sat_res_data['tilgung'] = data['payments']
        sat_res_data['futurecashflow'] = data['duration']
        sat_res_data['verwendungszweck'] = data['purpose'].apply(lambda x: self.lkp_verwendungszweck(x))
        sat_res_data['loeschung'] = 10
        sat_res_data['darlehen_hk'] = data['loan_hk']
        sat_res_data['load_domain'] = self.__class__.__name__.upper()
        return sat_res_data

    def writeToDB(self, data: pd.DataFrame):
        logger(logging_str='INFO: Entity ' + self.target, logging_class=self.load_domain, log_to_cli=self.cli_log,
               log_to_file=self.file_log, log_lvl='info')

        con = connect_to_db(layer=self.schema_trg)
        loader = ILoader(date=self.date, loader_type='datavault', loading_sat='s_darlehen', loading_entity=self.target,
                         target_connection=con,
                         schema=self.schema_trg, build_hash_key=False, load_domain=self.__class__.__name__.upper())
        loader.load(data=data)


entity = Darlehen(date='2018-12-31')
join = entity.join()
#entity.writeToDB(entity.mapping(entity.join()))
