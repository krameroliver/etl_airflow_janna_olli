import yaml
import pandas as pd
import numpy as np
from datetime import time

target = 'trans'
with open(r'../Configs/ENB/' + target + '.yaml') as file:
    documents = yaml.full_load(file)
target_fields = documents[target]['tables']['trans']['fields']
data_types=documents[target]['tables']['trans']['data_types']
rel_types = {}
parse_list = []
for k,i in enumerate(target_fields):
    if data_types[k] != 'DATUM':
        rel_types[i] = data_types[k]
    else:
        rel_types[i] = 'str'
        parse_list.append(i)

print(rel_types)
print(parse_list)



data=pd.read_csv(r'../../rawdata/ENB/2018-12-31/trans.csv', delimiter=',',header=0, dtype=rel_types,parse_dates=parse_list,infer_datetime_format=True)
#data['date'] = pd.to_datetime(data['date'],infer_datetime_format='%Y-%m-%d')


print(data[['fulldate', 'fulltime', 'fulldatewithtime', 'date']])

