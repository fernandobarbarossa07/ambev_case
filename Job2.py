from datetime import datetime
import json

import pandas as pd

today = datetime.now()

class get_json_and_normalize_silver():

    def __init__(self):
        '''Função de inicialização
        '''
        print('Inicializing the class')
        self.get_json_and_normalize()
        print('Data acquired')
        self.save_df_silver()
        print('Data saved')


    def get_json_and_normalize(self):
        '''Função que lê o Json diário e o normaliza,
        adicionando algumas colunas
        '''
        
        file_day = f'bronze/{today.year}_{today.month}_{today.day}_breweries.json'

        with open(file_day, 'r') as file:
            self.file_json_day = json.load(file)

        self.df = pd.json_normalize(self.file_json_day, 
                               record_path='data', 
                               meta = 'ingestion_date')
        
        self.df['country'] = self.df['country'].str.strip()
        self.df['country'] = self.df['country'].str.replace(' ', '')
        self.df['country_location'] = self.df['country']
        self.df['ingestion_date_partion'] = self.df['ingestion_date']
        

    def save_df_silver(self):
        '''Função que salva o dataframe
        na camada silver
        '''
        file_name_silver = f'silver/{today.year}_{today.month}_{today.day}_breweries.parquet'
        self.df.to_parquet('silver', partition_cols=['ingestion_date_partion' ,'country_location'])

get_json_and_normalize_silver()