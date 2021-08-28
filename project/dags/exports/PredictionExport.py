from utils.ReadEntity import ReadEntity


import pandas as pd
import os

class PredictionExport:

    def __init__(self,p_date:str=''):
        self.p_date = p_date
        if os.path.isdir(r'/data_exports/PredictionCore/'):
            self.out_path = r'/data_exports/PredictionCore/'
        else:
            self.out_path = r'../../data_exports/PredictionCore/'

    @property
    def mapping(self):
        trans = ReadEntity(p_date=self.p_date, layer='biz', entity_name='transaktion').read_entity

        konto = ReadEntity(p_date=self.p_date, layer='biz', entity_name='konto').read_entity

        trans_konto = ReadEntity(p_date=self.p_date, layer='biz', entity_name='trans_konto').read_entity

        data = trans.merge(trans_konto, how='inner', on='transaktion_hk').merge(konto, how='inner', on='konto_hk')
        data['ausfuehrungsdatum'] = pd.to_datetime(data['ausfuehrungsdatum'])
        data.set_index('ausfuehrungsdatum', inplace=True)

        data.sort_index(inplace=True)
        return data

    @property
    def export(self):
        exportdata = self.mapping
        exportdata.to_csv(path_or_buf=self.out_path,sep='\t')

report=PredictionExport().export
