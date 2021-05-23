# package import
import yaml
from sqlalchemy.engine import create_engine


def connect_to_db(layer:str=None):
    with open(r'Configs/Global/db.yaml') as file:
        documents = yaml.full_load(file)

    user = documents['database']['user']
    psw = documents['database']['password']
    port = documents['database']['port']
    dbname = documents['database']['db_name']
    host = documents['database']['host']
    db_type = documents['database']['type']

    if db_type is 'mariadb':
        con_profile = "mysql+pymysql://"+user+":"+psw+"@"+host+"/"+layer+"?charset=utf8mb4"
    elif db_type is 'postgres':
        con_profile = db_type + '://' + user + ':' + str(psw) + '@' + host + ':' + str(port) + '/' + dbname
    else:
        con_profile = "mysql+pymysql://" + user + ":" + psw + "@" + host + "/" + layer + "?charset=utf8mb4"
    con = create_engine(con_profile)
    # print(con)
    return con
