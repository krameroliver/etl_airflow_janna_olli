# Import Pakete
# import utils as u
# import utils as u
from dateutil.relativedelta import relativedelta
from datetime import datetime
from termcolor2 import colored

import pandas as pd
import yaml
try:
    from utils.TableReader import readTableFromDB
    from utils.db_connection import connect_to_db
    from utils.TechFields import add_technical_col
    from utils.db_loader import LoadtoDB
except ImportError:
    from project.dags.utils.TableReader import readTableFromDB
    from project.dags.utils.db_connection import connect_to_db
    from project.dags.utils.TechFields import add_technical_col
    from project.dags.utils.db_loader import LoadtoDB

class Gp:
    def __init__(self, date):
        self.date = date
        self.date_dt = datetime.strptime(date,'%Y-%m-%d')
        self.schema_trg = 'biz'
        self.target = 'geschaeftspartner'
        self.schema_src = 'src'
        self.src_client = 'client'
        self.src_disp = 'disposition'
        self.src_card = 'card'

    def join(self):
        client = readTableFromDB(db_con=connect_to_db(), date=self.date, schema=self.schema_src, t_name=self.src_client)
        dispo = readTableFromDB(db_con=connect_to_db(), date=self.date, schema=self.schema_src, t_name=self.src_disp)
        card = readTableFromDB(db_con=connect_to_db(), date=self.date, schema=self.schema_src, t_name=self.src_card)

        data = client.merge(dispo, how='inner', left_on='client_id', right_on='client_id').merge(card, how='inner',
                                                                                                 left_on='disp_id',
                                                                                                 right_on='disp_id')
        print(data.columns)
        return data

    def mapping(self, data: pd.DataFrame):
        with open(r'Configs/ENB/{entity}.yaml'.format(entity=self.target)) as file:
            documents = yaml.full_load(file)

        out_data = pd.DataFrame(columns=documents['geschaeftspartner']['fields'])
        out_data['kundennummer'] = data['client_id']
        out_data['sozialversicherungsnummer'] = data['social']
        out_data['geburtsdatum'] = data['fulldate_x']

        sex = {'Female': 0, 'Male': 1, 'Div': 2}
        anrede = {'Female': 'Frau', 'Male': 'Herr', 'Div': 'Mensch'}
        out_data['geschlecht'] = data['sex'].apply(lambda x: sex[x])
        out_data['anrede'] = data['sex'].apply(lambda x: anrede[x])

        out_data['vorname'] = data['first'] + ' ' + data['middle']
        out_data['nachname'] = data['last']

        avarage_years = {
            'Female': 84,
            'Male': 79,
            'Div': 82}

        out_data['sterbedatum'] = data['fulldate_x'] + data['sex'].apply(lambda x: relativedelta(years=avarage_years[x]))
        out_data['loeschung'] = data['processing_date_start_x'].apply(lambda x: x + relativedelta(years=10))
        counts = data.groupby(by=['client_id']).size().reset_index(name='counts')
        data = data.merge(counts,how='left',left_on='client_id',right_on='client_id')
        out_data['kreditkartenanzahl'] = data['counts']

        return out_data


    def writeToDB(self,data:pd.DataFrame):
        print(colored('INFO: Tabelle ' + self.target, color='green'))
        con = connect_to_db()
        with open(r'Configs/ENB/{entity}.yaml'.format(entity=self.target)) as file:
            documents = yaml.full_load(file)
        schema = documents['{entity}'.format(entity=self.target)]['layer']
        data = add_technical_col(data=data,t_name=self.target,date=self.date)
        load_to_db(data=data, db_con=con, t_name=self.target, date=self.date, schema=schema)
        print('--- Beladung Ende ---\n')