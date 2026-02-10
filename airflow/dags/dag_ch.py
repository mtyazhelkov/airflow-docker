from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

import sys
import os

# Поднимаемся на уровень выше от папки dags
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from scripts.test_script import insert_into_ch_new
 

dag=DAG(
    'test',
    schedule_interval='@daily',
    start_date=datetime(2024,1,1),
    max_active_runs=1
)

def insert_into_ch():
    #Здесь может быть питон код
    
    #sql
    ch_hook=ClickHouseHook(clickhouse_conn_id='clickhouse_default')
    ch_hook.execute('''INSERT INTO test(id,name) VALUES(3, 'MARIA')''')

#ClickhouseOperator. For sql queries only 
insert_data_chOperator = ClickHouseOperator(
    task_id='insert_data_ch',
    sql='''INSERT INTO test (id, name) VALUES(1,'Mark');''',
    clickhouse_conn_id='clickhouse_default',
    dag=dag,
)

insert_data_pyOperator=PythonOperator(
    task_id='insert_data_py',
    python_callable=insert_into_ch,
    dag=dag,
)

insert_data_pyOperator_new=PythonOperator(
    task_id='insert_data_new_py',
    python_callable=insert_into_ch_new,
    dag=dag,
)


insert_data_chOperator>>insert_data_pyOperator>>insert_data_pyOperator_new

