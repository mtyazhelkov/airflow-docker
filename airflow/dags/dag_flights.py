from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

import sys
import os
sys.path.insert(0, '/project')

from scripts.departure_flights import departure_flights

with DAG(
   'flights',
    schedule_interval='@daily',
    start_date=days_ago(1),
    #end_date=datetime(2027,1,1),
    max_active_runs=1
) as dag:
  
  task1=departure_flights()

task1