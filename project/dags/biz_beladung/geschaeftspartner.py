# Import Pakete
# import utils as u
# import utils as u

from datetime import datetime

import pandas as pd
import yaml
from dateutil.relativedelta import relativedelta
from termcolor2 import colored
from utils.DataVaultLoader import DataVaultLoader
# try:
from utils.TableReader import read_raw_sql_sat
from utils.TechFields import add_technical_col
from utils.db_connection import connect_to_db


# except ImportError:
# from project.dags.utils.TableReader import read_raw_sql
# from project.dags.utils.db_connection import connect_to_db
# from project.dags.utils.TechFields import add_technical_col
# from project.dags.utils.db_loader import LoadtoDB

class Gp:
    def __init__(self, date):
        self.date = date
        self.date_dt = datetime.strptime(date, '%Y-%m-%d')
        self.schema_trg = 'biz'
        self.target = 'geschaeftspartner'
        self.schema_src = 'src'
        self.src_client = 'client'
        self.src_disp = 'disposition'
        self.src_card = 'card'

    def join(self):
        client = read_raw_sql_sat(db_con=connect_to_db(layer=self.schema_src), date=self.date, schema=self.schema_src,
                                  t_name=self.src_client)
        dispo = read_raw_sql_sat(db_con=connect_to_db(layer=self.schema_src), date=self.date, schema=self.schema_src,
                                 t_name=self.src_disp)
        card = read_raw_sql_sat(db_con=connect_to_db(layer=self.schema_src), date=self.date, schema=self.schema_src,
                                t_name=self.src_card)

        data = client.merge(dispo, how='inner', left_on='client_id', right_on='client_id').merge(card, how='inner',
                                                                                                 left_on='disp_id',
                                                                                                 right_on='disp_id')
        return data

    def lkp_avarage_years(self, sex: str):
        avarage_years = {
            'Female': 84,
            'Male': 79,
            'Div': 82}

        return avarage_years[sex]

    def mapping(self, data: pd.DataFrame):

        out_data = pd.DataFrame()
        out_data['kundennummer'] = data['client_id']
        out_data['sozialversicherungsnummer'] = data['social']
        out_data['geburtsdatum'] = data['fulldate_x']

        sex = {'Female': 0, 'Male': 1, 'Div': 2}
        anrede = {'Female': 'Frau', 'Male': 'Herr', 'Div': 'Mensch'}
        out_data['geschlecht'] = data['sex'].apply(lambda x: sex[x])
        out_data['anrede'] = data['sex'].apply(lambda x: anrede[x])

        out_data['vorname'] = data['first'] + ' ' + data['middle']
        out_data['nachname'] = data['last']

        out_data['sterbedatum'] = data['fulldate_x'] + data['sex'].apply(lambda x: relativedelta(years=10))
        out_data['loeschung'] = 0
        counts = data.groupby(by=['client_id']).size().reset_index(name='counts')
        data = data.merge(counts, how='left', left_on='client_id', right_on='client_id')
        out_data['kreditkartenanzahl'] = data['counts']

        return out_data

    def mapping_postalischeadresse(self, data: pd.DataFrame):
        '''
        :param data:      Dataframe mit den gejointen Daten
        :return out_data: Dataframe mit den gemappten daten

        zu mappende felder:
        - 'kundennummer'
        - 'addresse1'
        - 'addresse2'
        - 'stadt'
        - 'bundesland'
        - 'postleitzahl'

        '''
        out_data = pd.DataFrame()
        out_data['kundennummer'] = data['client_id']
        out_data['addresse1'] = data['address_1']
        out_data['addresse2'] = data['address_2']
        out_data['stadt'] = data['city']
        out_data['bundesland'] = data['state']
        out_data['postleitzahl'] = data['zipcode']

        return out_data

    def mapping_digitaleadresse(self, data: pd.DataFrame):
        '''
        :param data: Dataframe der gejointen daten
        :return res_data: Dataframe mit den gemappten daten

        multi_satelit
        zu mappende felder:
        - 'kundennummer'
        - 'kontakttyp'
        - 'kontaktinfo'
        '''

        out_data = pd.DataFrame()

        '''
        Schritt 1: telefonnummer daten filtern
        '''
        phone_data = pd.DataFrame()
        teil_df = data[data['phone'].notnull()]
        phone_data['kundennummer'] = teil_df['client_id']
        phone_data['kontakttyp'] = 1
        phone_data['kontaktinfo'] = teil_df['phone']

        '''
        Schritt 2: email daten filtern
        '''
        mail_data = pd.DataFrame()
        teil_df = data[data['email'].notnull()]
        mail_data['kundennummer'] = teil_df['client_id']
        mail_data['kontakttyp'] = 2
        mail_data['kontaktinfo'] = teil_df['email']

        '''
        Schritt 3: Joinen der Teildaten
        '''
        out_data = pd.concat([phone_data, mail_data])
        return out_data

    def writeToDB(self, data: pd.DataFrame):
        print(colored('INFO: Entity ' + self.target, color='green'))
        con = connect_to_db(layer=self.schema_trg)
        sat_data = add_technical_col(data=data, t_name='s_geschaeftspartner', date=self.date, entity_name=self.target)
        with open(r'/Configs/ENB/' + self.target + '.yaml') as file:
            documents = yaml.full_load(file)
        hub_target_fields = documents[self.target]['tables']['h_' + self.target]['fields']
        hub_res_data = pd.DataFrame(columns=hub_target_fields)
        hub_res_data[hub_target_fields] = sat_data[hub_target_fields]

        dv_sat = DataVaultLoader(data=sat_data, db_con=con, entity_name=self.target, t_name='s_' + self.target,
                                 date=self.date, schema=self.schema_trg)
        dv_hub = DataVaultLoader(data=hub_res_data, db_con=con, entity_name=self.target, t_name='h_' + self.target,
                                 date=self.date, schema=self.schema_trg)
        dv_sat.load
        dv_hub.load

        print('--- Beladung Ende ---\n')

    def writeToDB_postalischeadd(self, data: pd.DataFrame):
        print(colored('INFO: Entity ' + self.target, color='green'))
        con = connect_to_db(layer=self.schema_trg)
        sat_data = add_technical_col(data=data, t_name='s_geschaeftspartner_postalische_addresse', date=self.date,
                                     entity_name=self.target)
        with open(r'/Configs/ENB/' + self.target + '.yaml') as file:
            documents = yaml.full_load(file)
        hub_target_fields = documents[self.target]['tables']['h_' + self.target]['fields']
        hub_res_data = pd.DataFrame(columns=hub_target_fields)
        hub_res_data[hub_target_fields] = sat_data[hub_target_fields]

        dv_sat = DataVaultLoader(data=sat_data, db_con=con, entity_name=self.target,
                                 t_name="s_geschaeftspartner_postalische_addresse",
                                 date=self.date, schema=self.schema_trg)
        dv_hub = DataVaultLoader(data=hub_res_data, db_con=con, entity_name=self.target, t_name='h_' + self.target,
                                 date=self.date, schema=self.schema_trg)
        dv_sat.load

        print('--- Beladung Ende ---\n')

    def writeToDB_digitale_add(self, data: pd.DataFrame):
        print(colored('INFO: Entity ' + self.target, color='green'))
        con = connect_to_db(layer=self.schema_trg)
        sat_data = add_technical_col(data=data, t_name='m_geschaeftspartner_digitale_addresse', date=self.date,
                                     entity_name=self.target)
        with open(r'/Configs/ENB/' + self.target + '.yaml') as file:
            documents = yaml.full_load(file)
        hub_target_fields = documents[self.target]['tables']['h_' + self.target]['fields']
        hub_res_data = pd.DataFrame(columns=hub_target_fields)
        hub_res_data[hub_target_fields] = sat_data[hub_target_fields]

        dv_sat = DataVaultLoader(data=sat_data, db_con=con, entity_name=self.target,
                                 t_name='m_geschaeftspartner_digitale_addresse',
                                 date=self.date, schema=self.schema_trg)
        dv_hub = DataVaultLoader(data=hub_res_data, db_con=con, entity_name=self.target, t_name='h_' + self.target,
                                 date=self.date, schema=self.schema_trg)
        dv_sat.load

        print('--- Beladung Ende ---\n')
