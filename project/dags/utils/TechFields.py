import pandas as pd
import yaml
import hashlib


def add_technical_col(data: pd.DataFrame,t_name:str,date:str=None):



    with open(r'/Configs/ENB/{entity}.yaml'.format(entity=t_name)) as file:
        documents = yaml.full_load(file)
    data = data[documents['{entity}'.format(entity=t_name)]['fields']]
    f = documents['{entity}'.format(entity=t_name)]['fields']

    data['diff_str'] = data.astype(str).agg('|'.join, axis=1)
    data["diff_hk"] = data['diff_str'].astype(str).apply(
        lambda x: hashlib.md5(x.encode()).hexdigest().upper())
    data.drop(inplace=True, columns='diff_str')
    data['record_source'] = 'ENB'
    if date is not None:
        data['processing_date_start'] = date
    data['mod_flg'] = 'I'

    # hash-key berechnen
    bkf = documents['{entity}'.format(entity=t_name)]['businesskeys']
    business_fields = documents['{entity}'.format(entity=t_name)]['fields']
    if len(bkf) == 1:
        data[t_name + "_hk"] = data[bkf[0]].astype(str).apply(
            lambda x: hashlib.md5(x.encode()).hexdigest().upper())
    else:
        data[t_name + '_str'] = data.astype(str).agg(''.join, axis=1)
        data[t_name + "_hk"] = data[t_name + '_str'].astype(str).apply(
            lambda x: hashlib.md5(x.encode()).hexdigest().upper())
        data.drop(inplace=True, columns=t_name + '_str')
    return data