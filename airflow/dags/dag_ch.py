from airflow import DAG
from airflow.utils.dates import days_ago
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

dag=DAG(
    'test',
    schedule_interval='@daily',
    start_date=datetime(2024,1,1),
    max_active_runs=1
)

create_table=ClickHouseOperator(
    task_id='create_table',
    sql='INSERT INTO test (id, name) VALUES(2,'Anton')',
    clickhouse_conn_id='clickhouse_default',
    dag=dag,
    )

create_table

