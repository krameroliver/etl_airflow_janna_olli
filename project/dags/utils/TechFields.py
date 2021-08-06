import pandas as pd
import yaml
import hashlib
import os


def add_technical_col(data: pd.DataFrame, t_name: str, date: str = None, entity_name: str = None):
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
    fields.append(documents[entity]['tables'][t_name]['hash_key'])
    data = data[fields]
    data['diff_str'] = data.astype(str).agg('|'.join, axis=1)
    data["diff_hk"] = data['diff_str'].astype(str).apply(
        lambda x: hashlib.md5(x.encode()).hexdigest().upper())
    data.drop(inplace=True, columns='diff_str')
    data['record_source'] = 'ENB'
    if date is not None:
        data['processing_date_start'] = date
    data['mod_flg'] = 'I'

    # hash-key berechnen

    # if len(bkf) == 1:
    #     data[hk_name] = data[bkf[0]].astype(str).apply(
    #         lambda x: hashlib.md5(x.encode()).hexdigest().upper())
    # else:
    #     data[t_name + '_str'] = data.astype(str).agg(''.join, axis=1)
    #     data[hk_name] = data[t_name + '_str'].astype(str).apply(
    #         lambda x: hashlib.md5(x.encode()).hexdigest().upper())
    #     data.drop(inplace=True, columns=t_name + '_str')
    return data
