from datetime import datetime
import json

import requests as re


today = datetime.now()

class get_and_save_data_breweries():

    def __init__(self):
        '''Função de inicialização
        '''
        print('Inicializing the class')
        self.get_data_breweries()
        print('Data acquired')
        self.save_json()
        print('Data saved')


    def get_data_breweries(self):
        '''Função que faz chamad a API breweries,
        fazendo a paginação necessária
        '''
        
        list_respose = []
        length = 1
        i = 0
        while length != 0:
            
            response = re.get(f'https://api.openbrewerydb.org/v1/breweries?page={i}&per_page=200')
            breweries = response.json() 
            i+=1
            length = len(breweries)
            list_respose.extend(breweries)

        
        date_today =f'{today.year}_{today.month}_{today.day}'
        self.data = {'ingestion_date':date_today,
            'data':list_respose}
        

    def save_json(self):
        '''Função que salva o Json
        na camada bronze
        '''
        file_name_raw = f'bronze/{today.year}_{today.month}_{today.day}_breweries.json'
        with open(file_name_raw, 'w') as file:
            json.dump(self.data, file)

get_and_save_data_breweries()