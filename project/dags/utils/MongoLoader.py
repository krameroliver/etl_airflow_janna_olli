import os

from pymongo import MongoClient
import pandas as pd
from termcolor2 import colored
import yaml

class MongoLoader:

    def __init__(self, report_name: str, bi_departement: str):
        self.report_name = report_name
        self.bi_department = bi_departement
        if os.path.isdir(r'/Configs/Global/'):
            self.conf_r = r'/Configs/Global/'
        else:
            self.conf_r = r'../Configs/Global/'

    def writereport(self, report_data: pd.DataFrame):
        with open(self.conf_r + 'db.yaml') as file:
            documents = yaml.full_load(file)
        mongodb_port = documents['database']['MongoDB']['port']
        mongodb_host = documents['database']['MongoDB']['host']
        db = documents['database']['MongoDB']['db_name'] + '_' + self.bi_department


        print(colored('Write Report: {0}.{1}'.format(db, self.report_name), 'green'))
        client = MongoClient(mongodb_host, mongodb_port)
        db = client[db]
        collection_currency = db[self.report_name]
        data = report_data.to_dict('record')
        collection_currency.insert(data)
        print(colored('Report {0}.{1} Loaded '.format(db, self.report_name), 'green'))
