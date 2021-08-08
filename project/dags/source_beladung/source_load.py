import pandas as pd
import os

import yaml
import platform
import warnings
warnings.filterwarnings("ignore")

try:
    from utils.DataVaultLoader import DataVaultLoader
    from utils.TableReader import read_raw_sql_sat
    from utils.TechFields import add_technical_col
    from utils.db_connection import connect_to_db
    from utils.ILoader import ILoader
except ImportError:
    from project.dags.utils.DataVaultLoader import DataVaultLoader
    from project.dags.utils.TableReader import read_raw_sql_sat
    from project.dags.utils.TechFields import add_technical_col
    from project.dags.utils.db_connection import connect_to_db
    from project.dags.utils.ILoader import ILoader


def get_os_pathes():
    '''
    Linux: Linux
    Mac: Darwin
    Windows: Windows
    '''

    sys_os = platform.system()
    if sys_os == 'Linux':
        return (r'../Configs/ENB/', r'../rawdata/ENB/')
    elif sys_os == 'Windows':
        c_path = os.path.join("..", "Configs", "ENB")
        s_path = os.path.join("..", '..', "rawdata", "ENB")
        return (c_path, s_path)


def read_write_source(file, date, table, delm, header):
    '''

    :param file: welche datei soll gelesen werden
    :param date: zu welchem datum soll die datei gelesen werden
    :param table: welche tabelle soll geschrieben werden
    :param delm: wie ist die datei separiert (Spalten)
    :param header: in welcher zeile ist der header
    :return: schreibt die SRC beladung generisch
    '''
    if os.path.isdir(r'/Configs/ENB/'):
        conf_r = r'/Configs/ENB/'
        source_path = r'/rawdata/ENB/'
    else:
        conf_r = r'/home/oliver/PycharmProjects/etl_airflow_janna_olli/project/dags/Configs/ENB'
        source_path = r'/mnt/nas/DataRepository/ENB'

    p = os.path.join(conf_r, table + '.yaml')  # .replace(r"\\\\",r"/")
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
                     date=date, build_hash_key=True)
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
