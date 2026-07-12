from airflow.decorators import task
from scripts.class_download import DownloadFlightsData
from scripts.class_token import TokenManager
from datetime import datetime, timedelta, time

import sys
import os

# Явно добавляем путь к папке, где лежит папка scripts
sys.path.insert(0, '/project')

#@task
def departure_flights():
     from scripts.class_download import DownloadFlightsData
     from scripts.class_token import TokenManager
    
     # 1. Получаем дату вчерашнего дня
     yesterday_dt = datetime.now().date() - timedelta(days=1)

     # 2. Получаем  первую и последнюю дату вчерашнего дня
     fst_ts=int(datetime.combine(yesterday_dt, time.min).timestamp())
     lst_ts=int(datetime.combine(yesterday_dt, time.max).timestamp())
   
     #Загрузка данных за вчерашний день
     URL= f"https://opensky-network.org/api/flights/departure?airport=EDDF&begin={fst_ts}&end={lst_ts}"
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
     #Создаем экземпляр класса DownloadFlightsData
     data=DownloadFlightsData(URL, query,1000)
     #Вызываем метод download_data для загрузки данных
     data.download_data()