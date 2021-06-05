import os,sys
import pandas as pd
from sqlalchemy import MetaData
from sqlalchemy import create_engine, insert
import pymysql
from sshtunnel import SSHTunnelForwarder



source_path = r"../rawdata/ENB/2018-12-31/"
data = pd.read_csv(os.path.join(source_path, 'card' + '.csv'), delimiter=',', header=0)

tunnel = SSHTunnelForwarder(('82.165.203.114', 22), ssh_password="Z4ykW#&q*5", ssh_username="root",
                                remote_bind_address=("127.0.0.1", 3306))
tunnel.start()
local_port = str(tunnel.local_bind_port)
c_str = "mysql+pymysql://dbuser:123456@127.0.0.1:"+local_port+"/src?charset=utf8mb4"
con = create_engine(c_str)


metadata = MetaData(bind=con,schema='src')
metadata.reflect(bind=con, schema='src')
print(metadata.sorted_tables)
for table in [i for i in reversed(metadata.sorted_tables) if "_hist" not in i.name and i.name == 'card']:
    target_table = table

print(target_table)

conn = con.connect()
data["card_hk"]=data["card_id"]
for row in range(data.shape[0]):
    record=data.iloc[row]
    #print(record)
    stmt = (
        insert(target_table).
        values(record)
    )
    print(stmt)
#stm = "INSERT INTO src.disposition (disp_id, client_id,account_id,user_type) VALUES (1, '1','1','1');"

    conn.execute(stmt)
conn.execute('commit')



conn.close()
tunnel.close()
print('closed')
exit()






