import yaml
import pandas as pd
import numpy as np
from datetime import time

from project.dags.utils.build_dynamic_lookup import dynamic_lkp
from project.dags.utils.build_static_lookup import static_lookup

dynamic_lkp().post_lkp(tablename='card', lookup_name='CARTTYPE', column='card_type')
dynamic_lkp().post_lkp(tablename='loan', lookup_name='STATUS', column='status')
dynamic_lkp().post_lkp(tablename='trans', lookup_name='cf_operation', column='operation')
dynamic_lkp().post_lkp(tablename='trans', lookup_name='payment_type', column='k_symbol')
dynamic_lkp().post_lkp(tablename='order', lookup_name='payment_type', column='k_symbol',if_exists='append')

static_lookup().build_lkp(lookup_name='SEX', lkp_data={'auspraegung': ['Female', 'Male', 'Div'], 'ID': [0, 1, 2]})
static_lookup().build_lkp(lookup_name='ANREDE',
                          lkp_data={'auspraegung': ['Female', 'Male', 'Div'], 'ID': ['Frau', 'Herr', 'Mensch']})
static_lookup().build_lkp(lookup_name='DURCHSCHNITTSALTER',
                          lkp_data={'auspraegung': ['Female', 'Male', 'Div'], 'ID': [84, 79, 82]})
static_lookup().build_lkp(lookup_name='USER_TYPE', lkp_data={'auspraegung': ['User', 'Owner', ''], 'ID': [2, 1, 0]})
