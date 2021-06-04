import os,sys
import pandas as pd
from sqlalchemy import MetaData
from sqlalchemy import create_engine, insert
import pymysql
from sshtunnel import SSHTunnelForwarder



source_path = r"../rawdata/ENB/2018-12-31/"
data = pd.read_csv(os.path.join(source_path, 'acct' + '.csv'), delimiter=',', header=1)

tunnel = SSHTunnelForwarder(('82.165.203.114', 22), ssh_password="Z4ykW#&q*5", ssh_username="root",
                                remote_bind_address=("127.0.0.1", 3306))
tunnel.start()
local_port = str(tunnel.local_bind_port)
c_str = "mysql+pymysql://dbuser:123456@127.0.0.1:"+local_port+"/src?charset=utf8mb4"
con = create_engine(c_str)


metadata = MetaData(bind=con,schema='src')
metadata.reflect(bind=con, schema='src')
#print(metadata.sorted_tables)
for table in [i for i in reversed(metadata.sorted_tables) if "_hist" not in i.name and i.name == 'disposition']:
    target_table = table

print(target_table)
stmt = (
    insert(target_table).
    values(disposition_hk="c",disp_id='1', client_id='1',account_id="1",user_type="1")
)
print(stmt)
#stm = "INSERT INTO src.disposition (disp_id, client_id,account_id,user_type) VALUES (1, '1','1','1');"
conn = con.connect()
conn.execute(stmt)
conn.execute('commit')



conn.close()
tunnel.close()
print('closed')
exit()






