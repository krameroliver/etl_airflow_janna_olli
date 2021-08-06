import pandas as pd
from sqlalchemy import MetaData

try:

    from utils.db_connection import connect_to_db

except ImportError:

    from project.dags.utils.db_connection import connect_to_db

class static_lookup:
    def __init__(self):
        pass

    def build_lkp(self,lookup_name: str, lkp_data):
        pd.DataFrame(data=lkp_data).to_sql(schema='biz', con=connect_to_db('biz'), if_exists='replace', name=lookup_name, index=False)

