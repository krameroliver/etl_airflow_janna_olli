import pandas as pd
import yaml
import hashlib
import os


def add_technical_col(data: pd.DataFrame, t_name: str, date: str = None, entity_name: str = None,buildHashKey:bool=False):
    if os.path.isfile(r'/Configs/ENB/{entity}.yaml'.format(entity=entity_name)):
        conf = r'/Configs/ENB/{entity}.yaml'.format(entity=entity_name)
    else:
        conf = r'../Configs/ENB/{entity}.yaml'.format(entity=entity_name)

    with open(conf) as file:
        documents = yaml.full_load(file)

    if entity_name is None:
        entity = t_name
    else:
        entity = entity_name

    fields = documents[entity]['tables'][t_name]['fields']
    _data = data[fields]
    data['diff_str'] = _data.astype(str).agg('|'.join, axis=1)
    data["diff_hk"] = data['diff_str'].astype(str).apply(
        lambda x: hashlib.md5(x.encode()).hexdigest().upper())
    data.drop(inplace=True, columns='diff_str')

    data['record_source'] = 'ENB'
    if date is not None:
        data['processing_date_start'] = date
    data['mod_flg'] = 'I'

    # hash-key berechnen
    if buildHashKey:
        hk_name = documents[entity]['tables'][t_name]['hash_key']
        bks = documents[entity]['tables'][t_name]['businesskeys']

        data['help'] = data[bks].astype(str).agg('|'.join, axis=1)
        data[hk_name] = data['help'].apply(
        lambda x: hashlib.md5(x.encode()).hexdigest().upper())

    return data
