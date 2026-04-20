from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook


import sys
import os

# Явно добавляем путь к папке, где лежит папка scripts
sys.path.insert(0, '/project')
from scripts.test_sript import insert_into_ch_new


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
    pg_hook.run('''insert into test_pg (id, user_name) VALUES(5, 'Juanna')''')


insert_data_pyOperator=PythonOperator(
    task_id='insert_data_py',
    python_callable=insert_into_pg,
    dag=dag,
)


insert_data_pyOperator

