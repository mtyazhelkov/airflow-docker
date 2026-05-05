import requests
from datetime import datetime, timedelta
import json
import psycopg2
from scripts.class_token import TokenManager
from airflow.providers.postgres.hooks.postgres import PostgresHook


import sys
import os

# Явно добавляем путь к папке, где лежит папка scripts
sys.path.insert(0, '/project')

class DownloadFlightsData:
    def __init__(self, URL, query, batch_size):
        #Вызываю класс TokenManager, создав его экземпляр внутри DownloadFlightsData 
        self.tokens=TokenManager()   
        self.URL=URL
        self.query=query
        self.batch_size=batch_size
    
    def get_response(self):
        response = requests.get(self.URL,
                                headers=self.tokens.headers(),
                                )
        response.raise_for_status() #Проверка на ошибки (404, 500 и т.д.)
        return response.json()

    def download_data(self): 
        flights_data=self.get_response()

        #делим входящие данные на батчи
        def get_batch(data,size):
            for i in range(0,len(data), size):
                yield data[i:i+size]
        
     
        for batch in get_batch(flights_data, self.batch_size):
            conn=None
            json_data = json.dumps(batch) # Превращаем список словарей в одну строку JSON
            pg_hook=PostgresHook(postgres_conn_id='postgres_new')
            pg_hook.run(self.query, parameters=(json_data,))
        # try:
        #      conn=psycopg2.connect(**db_params)
        #      with conn.cursor() as curr:
        #           curr.execute(query, (json.dumps(data),))
        #           conn.commit()
        #           print(f"Успешно загружено {len(data)}")
        # except Exception as e:

        #      print(f"Ошибка работы с БД {e}")
        #      if conn:
        #         conn.rollback()
        # finally:
        #         conn.close()
        #         print("Соединение с БД закрыто")