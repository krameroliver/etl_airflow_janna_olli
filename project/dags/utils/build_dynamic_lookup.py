import pandas as pd
from sqlalchemy import MetaData

try:

    from utils.db_connection import connect_to_db

except ImportError:

    from project.dags.utils.db_connection import connect_to_db


class dynamic_lkp:
    def __init__(self, tablename: str, lookup_name: str, column: str):
        self.tablename = tablename
        self.column = column
        self.lookup_name = lookup_name
        self.build_lkp()

    def build_lkp(self):
        db_con_src = connect_to_db(layer='src')

        metadata = MetaData(bind=db_con_src)
        metadata.reflect(bind=db_con_src, schema='src')

        stmt = "select distinct {field} from src.{table}".format(field=self.column, table=self.tablename)
        res = db_con_src.execute(stmt)
        data = [i[0] for i in sorted(res.fetchall())]
        ids = [i for i in range(len(data))]

        self.lkp_data = pd.DataFrame(columns=['auspraegung', 'ID'], data={'ID': ids, 'auspraegung': data})

    @property
    def post_lkp(self):
        db_con_biz = connect_to_db(layer='biz')
        metadata = MetaData(bind=db_con_biz)
        metadata.reflect(bind=db_con_biz, schema='biz')
        self.lkp_data.to_sql(schema='biz', con=db_con_biz, if_exists='replace', name=self.lookup_name.upper(),
                             index=False)


lkp = dynamic_lkp('trans', 'trans', 'k_symbol')
lkp.post_lkp
