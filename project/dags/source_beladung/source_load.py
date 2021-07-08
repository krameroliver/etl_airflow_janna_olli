import pandas as pd
import os

import yaml
import platform

try:
    from utils.DataVaultLoader import DataVaultLoader
    from utils.TableReader import read_raw_sql_sat
    from utils.TechFields import add_technical_col
    from utils.db_connection import connect_to_db
except ImportError:
    from project.dags.utils.DataVaultLoader import DataVaultLoader
    from project.dags.utils.TableReader import read_raw_sql_sat
    from project.dags.utils.TechFields import add_technical_col
    from project.dags.utils.db_connection import connect_to_db


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
        s_path = os.path.join("..",'..', "rawdata", "ENB")
        return (c_path, s_path)


def read_write_source(file, date, table, delm, header):
    if os.path.isdir(r'/Configs/ENB/'):
        conf_r = r'/Configs/ENB/'
        source_path = r'/rawdata/ENB/'
    else:
        conf_r = r'../Configs/ENB/'
        source_path = r'../../rawdata/ENB/'

    print(os.path.abspath(os.getcwd()))
    p = os.path.join(conf_r, table + '.yaml').replace(r"\\\\",r"/")
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
    data = add_technical_col(data=data, t_name=table, date=date, entity_name=table)

    con = connect_to_db(layer=layer)
    dvl = DataVaultLoader(data=data, t_name=table, date=date, db_con=con, entity_name=table, schema=layer,
                          commit_size=10000)
    dvl.load
