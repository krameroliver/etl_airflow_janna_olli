# package import
import yaml
from sqlalchemy.engine import create_engine
from sshtunnel import SSHTunnelForwarder


def connect_to_db(layer:str=None):

    with open(r'../Configs/Global/db.yaml') as file:
        documents = yaml.full_load(file)

    user = documents['database']['user']
    psw = documents['database']['password']
    port = documents['database']['port']
    dbname = documents['database']['db_name']
    host = documents['database']['host']
    db_type = documents['database']['type']
    ssh_host = documents['database']['sshhost']
    ssh_user = documents['database']['sshuser']
    ssh_pwd = documents['database']['sshpw']

    tunnel = SSHTunnelForwarder((ssh_host, 22), ssh_password=ssh_pwd, ssh_username=ssh_user,
                                remote_bind_address=(host, port))
    tunnel.start()
    local_port = str(tunnel.local_bind_port)
    c_str = "mysql+pymysql://"+str(user)+":"+str(psw)+"@"+str(host)+":" + str(local_port) + "/"+str(layer)+"?charset=utf8mb4"
    con = create_engine(c_str)

    # print(con)
    return con
