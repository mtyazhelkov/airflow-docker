from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
#from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook


import sys
import os

# Явно добавляем путь к папке, где лежит папка scripts
sys.path.insert(0, '/project')
from scripts.test_sript import insert_into_ch_new
from scripts.class_token import TokenManager
from scripts.class_download import DownloadFlightsData

URL= "https://opensky-network.org/api/flights/departure?airport=EDDF&begin=1774828800&end=1774915199"
query='''
             -- Используем json.dumps для превращения списка в одну JSON-строку
             -- jsonb_populate_recordset автоматически разложит ключи по колонкам таблицы flights.tb_departures
             insert into flights.tb_departures (
                  icao24,
                  firstseen,
                  est_departure_airport,
                  lastseen,
                  est_arrival_airport,
                  callsign,
                  est_departure_airport_horiz_distance,
                  est_departure_airport_vert_distance,
                  est_arrival_airport_horiz_distance,
                  est_arrival_airport_vert_distance,
                  departure_airport_candidates_count,
                  arrival_airport_candidates_count         
             )
             select 
             "icao24" ,
             "firstSeen",
             "estDepartureAirport",
             "lastSeen",
             "estArrivalAirport",
             "callsign",
             "estDepartureAirportHorizDistance",
             "estDepartureAirportVertDistance",
             "estArrivalAirportHorizDistance",
             "estArrivalAirportVertDistance",
             "departureAirportCandidatesCount",
             "arrivalAirportCandidatesCount"   
             from jsonb_to_recordset(%s)
             AS x(
                  "icao24" text,
                  "firstSeen" INT,
                  "estDepartureAirport" text,
                  "lastSeen" INT,
                  "estArrivalAirport" text,
                  "callsign" text,
                  "estDepartureAirportHorizDistance" INT,
                  "estDepartureAirportVertDistance" INT,
                  "estArrivalAirportHorizDistance" INT,
                  "estArrivalAirportVertDistance" INT,
                  "departureAirportCandidatesCount" INT,
                  "arrivalAirportCandidatesCount" INT     
                 )
                 ON CONFLICT (icao24, firstseen) DO NOTHING;
             ;   
               '''

def download_data(URL, query):
    #Создаем экземпляр класса DownloadFlightsData
    data=DownloadFlightsData(URL, query,1000
    )
    #Вызываем метод download_data для загрузки данных
    data.download_data()

dag=DAG(
    'test_2',
    schedule_interval='@daily',
    start_date=days_ago(1),
    #end_date=datetime(2027,1,1),
    max_active_runs=1
)

def insert_into_pg():
    #Здесь может быть питон код
    
    #sql
    pg_hook=PostgresHook(postgres_conn_id='postgres_new')
    pg_hook.run('''insert into test_pg (user_name) VALUES('Mary') ON CONFLICT (id) DO UPDATE SET user_name = EXCLUDED.user_name ''')


insert_data_pyOperator=PythonOperator(
    task_id='insert_data_py',
    python_callable= download_data,
    op_kwargs={'URL': URL, 'query': query},  # Передаем переменные в функцию
    dag=dag,
)


insert_data_pyOperator

