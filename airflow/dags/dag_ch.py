from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

def insert_to_clickhouse(csv_file, table_name):
    ch_hook = ClickHouseHook(clickhouse_conn_id='clickhouse_default')
    ch_hook.execute('''INSERT INTO test (id, name) VALUES(2,'Anton');''')   

dag=DAG(
    'test',
    schedule_interval='@daily',
    start_date=datetime(2024,1,1),
    max_active_runs=1
)

insert_data=ClickHouseOperator(
    task_id='create_table',
    python_callable=insert_to_clickhouse,
    dag=dag,
    )

insert_data

