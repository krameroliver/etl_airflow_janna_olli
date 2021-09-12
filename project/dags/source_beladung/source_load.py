import warnings

import pandas as pd
import yaml

warnings.filterwarnings("ignore")

from dwhutils.db_connection import connect_to_db
from dwhutils.ILoader import ILoader
import os
from dotenv import load_dotenv


def read_write_source(file, date, table, delm, header):
    '''

    :param file: welche datei soll gelesen werden
    :param date: zu welchem datum soll die datei gelesen werden
    :param table: welche tabelle soll geschrieben werden
    :param delm: wie ist die datei separiert (Spalten)
    :param header: in welcher zeile ist der header
    :return: schreibt die SRC beladung generisch
    '''

    load_dotenv()

    conf_r = os.getenv('ENTITY_CONFIGS')
    source_path = os.getenv('DATA_PATH')

    p = os.path.join(conf_r, table + '.yaml').replace(r"\\\\", r"/")
    with open(p) as f:
        documents = yaml.full_load(f)
    target_fields = documents[table]['tables'][table]['fields']
    data_types = documents[table]['tables'][table]['data_types']
    layer = documents[table]['tables'][table]['layer']

    rel_types = {}
    parse_list = []
    for k, i in enumerate(target_fields):
        if data_types[k] != 'DATUM':
            rel_types[i] = data_types[k]
        else:
            rel_types[i] = 'str'
            parse_list.append(i)

    source_path = os.path.join(source_path, date, file)
    data = pd.read_csv(source_path, delimiter=delm, header=header, dtype=rel_types, parse_dates=parse_list,
                       infer_datetime_format=True)
    data.fillna(value="", inplace=True)

    con = connect_to_db(layer=layer)
    loader = ILoader(loading_sat=table, loader_type='flat', loading_entity=table, target_connection=con, schema='src',
                     date=date, build_hash_key=True,load_domain='ENB')
    loader.load(data=data)


read_write_source(file='acct.csv', date="2018-12-31", table='acct', header=0, delm=',')
read_write_source(file='card.csv', date="2018-12-31", table='card', header=0, delm=',')
read_write_source(file='client.csv', date="2018-12-31", table='client', header=0, delm=',')
read_write_source(file='disposition.csv', date="2018-12-31", table='disposition', header=0, delm=',')
read_write_source(file='district.csv', date="2018-12-31", table='district', header=0, delm=',')
read_write_source(file='loan.csv', date="2018-12-31", table='loan', header=0, delm=',')
read_write_source(file='order.csv', date="2018-12-31", table='order', header=0, delm=',')
print('trans')
read_write_source(file='trans.csv', date="2018-12-31", table='trans', header=0, delm=',')
print('Fertig')
