import yaml
import pandas as pd
import numpy as np
import datetime
target = 'trans'
with open(r'../Configs/ENB/' + target + '.yaml') as file:
    documents = yaml.full_load(file)
target_fields = documents[target]['tables']['trans']['fields']
data_types=documents[target]['tables']['trans']['data_types']
types={target_fields[i]:data_types[i] for i in range(len(target_fields))}
print(data_types)

data=pd.read_csv(r'../../rawdata/ENB/2018-12-31/trans.csv', delimiter=';',header=0, dtype=types,parse_dates=['fulldate', 'fulltime', 'fulldatewithtime'])

print(data.dtypes)

