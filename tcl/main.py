import sys
import paramiko
import sqlite3
from sqlite3 import Error

import datetime
from stat import S_ISDIR

arguments = sys.argv[1:]

dir = arguments[0]


def settings_get(direction):
    conf = {}
    with open(f'{direction}') as fp:
        for line in fp:
            key, val = line.strip().split('=')
            conf[key] = val
    return conf


def create_connection(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()


settings = settings_get(f"{dir}")

lpath = str(settings['local_dir'])
rpath = str(settings['sftp_remote_dir'])
host = str(settings["sftp_host"])
port = int(settings['sftp_port'])

username = str(settings["sftp_user"])
password = str(settings["sftp_password"])

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(hostname=host, username=username, password=password, port=port)
sftp_client = ssh.open_sftp()


def isdir(path):
    try:
        return S_ISDIR(sftp_client.stat(path).st_mode)
    except IOError:
        # Path does not exist, so by definition not a directory
        return False


files = sftp_client.listdir("/bots/other/ru/test")

db_name = settings["sql_database"]
create_connection(f"{db_name}.db")

connection = sqlite3.connect(f'{db_name}.db')
cursor = connection.cursor()
cursor.execute('''CREATE TABLE IF NOT EXISTS logs
              (date TEXT, time TEXT, file_name TEXT)''')

for file_name in files:
    if not isdir(f"{rpath}/{file_name}"):
        now = datetime.datetime.now()

        date = now.strftime('%Y-%m-%d')
        time = now.strftime('%H:%M:%S')

        sftp_client.get(f'{rpath}/{file_name}', f'{lpath}/{file_name}')

        cursor.execute(f"INSERT INTO logs VALUES ('{date}', '{time}','{file_name}')")
        connection.commit()

        print("{:15} {:15} {:10}".format(file_name, date, time))

sftp_client.close()
ssh.close()
