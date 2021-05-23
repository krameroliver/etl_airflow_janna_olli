from utils.TableReader import readTableFromDB
from utils.db_connection import connect_to_db


test = readTableFromDB(schema="src",db_con=connect_to_db(),t_name="acct",date='2018-12-31')
print(test)