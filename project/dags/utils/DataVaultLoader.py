import pandas as pd
import yaml
from termcolor2 import colored

from utils.db_connection import connect_to_db
from utils.db_loader import LoadtoDB


class DataVaultLoader():

    def __init__(self, data: pd.DataFrame, db_con, t_name, date: str = None, schema: str = None,
                 commit_size: int = 10000):
        self.data = data
        self.db_con = db_con
        self.t_name = t_name
        self.date=date
        self.schema = schema
        self.commit_size = commit_size
        self.target_table = None


        with open(r'/Configs/ENB/' + self.t_name + '.yaml') as file:
            self.documents = yaml.full_load(file)
        self.table_type = self.documents[self.t_name]['table_type']

        self.Loader=LoadtoDB(data=self.data,db_con=self.db_con, t_name=self.t_name,date=self.date, schema=self.schema,commit_size=self.commit_size)

    @property
    def load(self):
        if self.table_type=="satellit":
            self.satellit()
        elif self.table_type=="hub":
            self.hub()
        elif self.table_type=="link":
            self.link()
        else:
            print(colored('ERROR:', color='red')+' Kein Zulaessiger Tabellen-Typ gefunden')



    def satellit(self):
        self.Loader.insert()
        self.Loader.update()
        self.Loader.delete()

    def hub(self):
        self.Loader.insert()

    def link(self):
        self.Loader.insert()


    def __repr__(self):
        return(self.Loader.__repr__())



# con = connect_to_db(layer="src")
#
# source_path = r"../../rawdata/ENB/2018-12-31/"
# data = pd.read_csv(os.path.join(source_path, 'card' + '.csv'), delimiter=',', header=0)
# data["card_hk"] = data['card_id']
# #data=data.iloc[0:10,:]
# l=DataVaultLoader(data=data, db_con=con, t_name="card", date="2018-12-31", schema="src")
# l.load
# #l.insert()
# #l.update()
# #l.delete()
# print(l)


