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
from termcolor2 import colored


# try:
#    from utils.DataVaultLoader import DataVaultLoader
#    from utils.TableReader import read_raw_sql_sat
#    from utils.TechFields import add_technical_col
#    from utils.db_connection import connect_to_db
#    from utils.ILoader import ILoader
# except ImportError:
#    from project.dags.utils.DataVaultLoader import DataVaultLoader
#    from project.dags.utils.TableReader import read_raw_sql_sat
#    from project.dags.utils.TechFields import add_technical_col
#    from project.dags.utils.db_connection import connect_to_db
#    from project.dags.utils.ILoader import ILoader

class Gp:
    def __init__(self, date):
        self.date = date
        self.date_dt = datetime.strptime(date, '%Y-%m-%d')
        self.schema_trg = 'biz'
        self.target = 'geschaeftspartner'
        self.schema_src = 'src'
        self.src_client = 'client'
        self.src_disp = 'disposition'
        self.load_domain = self.__class__.__name__.upper()
        self.src_card = 'card'
        self.conf_r = os.getenv('ENTITY_CONFIGS')
        self.doc_src = os.path.join(self.conf_r, self.src_card + '.yaml')
        self.doc_trg = os.path.join(self.conf_r, self.target + '.yaml')

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
        out_data['load_domain'] = self.load_domain

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
        out_data['geschaeftspartner_hk'] = data['client_id'].apply(
            lambda x: hashlib.md5(x.encode()).hexdigest().upper())

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
        out_data['load_domain'] = self.load_domain
        out_data['geschaeftspartner_hk'] = data['client_id'].apply(
            lambda x: hashlib.md5(x.encode()).hexdigest().upper())
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
        '''
        Schritt 1: telefonnummer daten filtern
        '''
        phone_data = pd.DataFrame()
        teil_df = data[data['phone'].notnull()]
        phone_data['kundennummer'] = teil_df['client_id']
        phone_data['kontakttyp'] = 1
        phone_data['kontaktinfo'] = teil_df['phone']
        phone_data['load_domain'] = self.load_domain
        phone_data['geschaeftspartner_hk'] = data['client_id'].apply(
            lambda x: hashlib.md5(x.encode()).hexdigest().upper())
        '''
        Schritt 2: email daten filtern
        '''
        mail_data = pd.DataFrame()
        teil_df = data[data['email'].notnull()]
        mail_data['kundennummer'] = teil_df['client_id']
        mail_data['kontakttyp'] = 2
        mail_data['kontaktinfo'] = teil_df['email']
        mail_data['load_domain'] = self.load_domain
        mail_data['geschaeftspartner_hk'] = data['client_id'].apply(
            lambda x: hashlib.md5(x.encode()).hexdigest().upper())
        '''
        Schritt 3: Joinen der Teildaten
        '''
        out_data = pd.concat([phone_data, mail_data])
        return out_data

    def writeToDB(self, data: pd.DataFrame):
        print(colored('INFO: Entity ' + self.target, color='green'))
        con = connect_to_db(layer=self.schema_trg)

        loader = ILoader(date=self.date, loader_type='datavault',
                         loading_sat='s_geschaeftspartner',
                         loading_entity=self.target,
                         target_connection=con,
                         schema=self.schema_trg, load_domain=self.load_domain)
        loader.load(data=data)

        print('--- Beladung Ende ---\n')

    def writeToDB_postalischeadd(self, data: pd.DataFrame):
        print(colored('INFO: Entity ' + self.target, color='green'))
        con = connect_to_db(layer=self.schema_trg)

        loader = ILoader(date=self.date, loader_type='datavault',
                         loading_sat='s_geschaeftspartner_postalische_addresse',
                         loading_entity=self.target,
                         target_connection=con,
                         schema=self.schema_trg, load_domain=self.load_domain)
        loader.load(data=data)

        print('--- Beladung Ende ---\n')

    def writeToDB_digitale_add(self, data: pd.DataFrame):
        print(colored('INFO: Entity ' + self.target, color='green'))
        con = connect_to_db(layer=self.schema_trg)
        loader = ILoader(date=self.date, loader_type='datavault', loading_sat='m_geschaeftspartner_digitale_addresse',
                         loading_entity=self.target,
                         target_connection=con,
                         schema=self.schema_trg, load_domain=self.load_domain)
        loader.load(data=data)

        print('--- Beladung Ende ---\n')


# entity = Gp(date='2018-12-31')
# entity.writeToDB(entity.mapping(entity.join()))
#
# entity = Gp(date='2018-12-31')
# entity.writeToDB_postalischeadd(entity.mapping_postalischeadresse(entity.join()))
#
# entity = Gp(date='2018-12-31')
# entity.writeToDB_digitale_add(entity.mapping_digitaleadresse(entity.join()))
