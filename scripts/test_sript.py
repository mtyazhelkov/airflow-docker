# /project/scripts/test_sript.py
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

def insert_into_ch_new():
    #Здесь может быть питон код
    print(f"Доступные имена: {globals().keys()}")
    #sql
    ch_hook=ClickHouseHook(clickhouse_conn_id='clickhouse_default')
    ch_hook.execute('''INSERT INTO test(id,name) VALUES(3, 'Kristina')''')