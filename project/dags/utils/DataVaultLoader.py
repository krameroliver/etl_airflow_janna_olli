import os

import pandas as pd
import yaml
from termcolor2 import colored
import logging

try:
    from utils.db_connection import connect_to_db
    from utils.db_loader import LoadtoDB
except ImportError:
    from project.dags.utils.db_connection import connect_to_db
    from project.dags.utils.db_loader import LoadtoDB


class DataVaultLoader():

    def __init__(self, data: pd.DataFrame, db_con, entity_name: str, t_name: str, date: str = None, schema: str = None,
                 commit_size: int = 10000):
        self.data = data
        self.db_con = db_con
        self.entity_name = entity_name
        self.t_name = t_name
        self.date = date
        self.schema = schema
        self.commit_size = commit_size
        self.target_table = None

        if os.path.isfile(r'/Configs/ENB/' + self.entity_name + '.yaml'):
            conf = r'/Configs/ENB/' + self.entity_name + '.yaml'
        else:
            conf = r'../Configs/ENB/' + self.entity_name + '.yaml'

        with open(conf) as file:
            self.documents = yaml.full_load(file)
        self.table_type = self.documents[entity_name]['tables'][self.t_name]['table_type']

        self.Loader = LoadtoDB(data=self.data, db_con=self.db_con, t_name=self.t_name, date=self.date,
                               schema=self.schema, commit_size=self.commit_size, entityName=self.entity_name)

    @property
    def load(self):
        if self.table_type == "satellit":
            self.satellit()
        elif self.table_type == "hub":
            self.hub()
        elif self.table_type == "link":
            self.link()
        else:
            print(colored('ERROR:', color='red') + ' Kein Zulaessiger Tabellen-Typ gefunden')

    def satellit(self):
        self.Loader.insert()
        self.Loader.update_v2()
        #self.Loader.update()
        self.Loader.delete()

    def hub(self):
        try:
            self.Loader.insert()
        except:
            logging.warn('inserts not done!')

    def link(self):
        try:
            self.Loader.insert()
        except:
            logging.warn('inserts not done!')

    def __repr__(self):
        return (self.Loader.__repr__())

