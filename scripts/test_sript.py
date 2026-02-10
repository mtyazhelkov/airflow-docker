def insert_into_ch_new():
    #Здесь может быть питон код
    
    #sql
    ch_hook=ClickHouseHook(clickhouse_conn_id='clickhouse_default')
    ch_hook.execute('''INSERT INTO test(id,name) VALUES(3, 'Kristina')''')