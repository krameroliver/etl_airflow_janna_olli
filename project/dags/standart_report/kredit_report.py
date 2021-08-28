import pandas as pd

try:
    from utils.DataVaultLoader import DataVaultLoader
    from utils.TableReader import read_raw_sql_sat
    from utils.TechFields import add_technical_col
    from utils.db_connection import connect_to_db
    from utils.lookup import get_lkp_value
    from utils.ILoader import ILoader
    from utils.ReadEntity import ReadEntity
except ImportError:
    from project.dags.utils.DataVaultLoader import DataVaultLoader
    from project.dags.utils.TableReader import read_raw_sql_sat
    from project.dags.utils.TechFields import add_technical_col
    from project.dags.utils.db_connection import connect_to_db
    from project.dags.utils.lookup import get_lkp_value
    from project.dags.utils.ILoader import ILoader
    from project.dags.utils.ReadEntity import ReadEntity


class KreditReport:

    def __init__(self, p_date: str = None):
        self.p_date = p_date

    @property
    def join_data(self):
        re_darlehen = ReadEntity(p_date=self.p_date, layer='biz', entity_name='darlehen')
        darlehen = re_darlehen.read_entity

        re_konto = ReadEntity(p_date=self.p_date, layer='biz', entity_name='konto')
        konto = re_konto.read_entity

        re_gp = ReadEntity(p_date=self.p_date, layer='biz', entity_name='geschaeftspartner')
        gp = re_gp.read_entity

        darlehen_konto = ReadEntity(p_date=self.p_date, layer='biz', entity_name='darlehen_konto').read_entity
        gp_konto = ReadEntity(p_date=self.p_date, layer='biz', entity_name='gp_konto').read_entity

        data = darlehen.merge(darlehen_konto, how='inner', on='darlehen_hk').merge(
            konto, how='inner',
            on='konto_hk').merge(gp_konto,
                                 how='inner',
                                 on='konto_hk').merge(
            gp, how='inner', on='geschaeftspartner_hk')

        return data

    def write_report(self, data: pd.DataFrame):
        loader = ILoader(date=self.p_date, loader_type='MONGO', report_name='KreditReport',
                         bi_departement='PoeticDrunkenCat', load_domain=self.__class__.__name__)
        loader.load(data)


report = KreditReport(p_date='2018-12-31')
report.write_report(report.join_data)
