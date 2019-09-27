import os
import pyodbc
import configparser
import requests
import xml.etree.ElementTree as ET
import urllib.request
from multiprocessing.dummy import Pool
from concurrent.futures import ThreadPoolExecutor
import time
from azure.storage.blob import BlockBlobService, PublicAccess
import glob
import shutil

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# read config file
def read_config():
    config = configparser.ConfigParser()
    config.read(os.path.join(BASE_DIR, 'eva-ecard/config.ini'))
    return config

# function for creating a database connection
def create_connection():
        config = read_config()
        driver = config['SQL_Credentials']['driver']
        server = config['SQL_Credentials']['server']
        database  = config['SQL_Credentials']['database']
        uid = config['SQL_Credentials']['uid']
        password = config['SQL_Credentials']['password']
        connection = pyodbc.connect("driver={};server={};database={};uid={};PWD={}".format(driver, server, database, uid, password),autocommit=True)
        return connection



connection = create_connection()
cursor = connection.cursor()
usernames = cursor.execute("select employeeID from degreedAllUsers")

with open('users.txt', 'w') as file:
    for user in usernames.fetchall():
        user  = user[0]
        file.write('{}\n'.format(user))