from datetime import datetime
import os

import pandas as pd

today = datetime.now()

class get_df_create_smart_table_gold():

    def __init__(self):
        '''Função de inicialização
        '''
        print('Inicializing the class')
        self.get_df_and_create_smart_table()
        print('Data acquired')
        self.save_df_gold()
        print('Data saved')


    def get_df_and_create_smart_table(self):
        '''Função que lê o Json diário e o normaliza,
        adicionando algumas colunas
        '''
        
        event = f'silver/ingestion_date_partion={today.year}_{today.month}_{today.day}'
        list_countries = os.listdir(event)
        list_countries
        
        self.df = pd.DataFrame()
        for i in list_countries:
            countries = f'silver/ingestion_date_partion={today.year}_{today.month}_{today.day}/{i}'
            list_files = os.listdir(countries)
            for j in list_files:
                countries_folder = f'{countries}/{j}'
                
                with open(countries_folder, 'r') as file:
                    df_parquet_day = pd.read_parquet(countries_folder)
                    
                self.df = pd.concat([df_parquet_day, self.df])
        self.df = self.df.groupby(['brewery_type','country'])['id'].count().reset_index()
        self.df.rename(columns={'id': "quantity_per_type_location"}, 
                       inplace = True)

    def save_df_gold(self):
        '''Função que salva o dataframe
        na camada gold
        '''
        file_name_gold = f'gold/{today.year}_{today.month}_{today.day}_breweries.parquet'
        self.df.to_parquet(file_name_gold)

get_df_create_smart_table_gold()