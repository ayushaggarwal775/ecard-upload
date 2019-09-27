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
import logging.handlers

# Create a log handler
handler = logging.handlers.WatchedFileHandler(
    os.environ.get("LOGFILE", "errors.log"))
formatter = logging.Formatter(logging.BASIC_FORMAT)
handler.setFormatter(formatter)
root = logging.getLogger()
root.setLevel(os.environ.get("LOGLEVEL", "INFO"))
root.addHandler(handler)

BASE_DIR = os.path.dirname(__file__)
# read config file
def read_config():
    config = configparser.ConfigParser()
    config.read(os.path.join(BASE_DIR+ '/config.ini'))
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

class FetchData:
    def __init__(self):
        self.usernames = []
        self.count = 0

    # fetch all usernames from database
    def fetch_usernames(self):
        dbconnection = create_connection()
        cursor = dbconnection.cursor()
        usernames = cursor.execute("select employeeID from degreedAllUsers")
        for username in usernames.fetchall():
            self.usernames.append(username[0])
        
    # fetch ecard for a single user
    def fetch_ecard(self, username):
        config = read_config()
        
        # create folder
        try:
            target_path = username + '/2019/'
            os.makedirs(os.path.dirname(target_path), exist_ok=True)
        except Exception as e:
            logging.exception('error in creating directory '+e)   
         
        # FOR SF API
        try:
            # Prepare request
            payload = "<DataRequest><groupcode>TCL</groupcode><employeeno>{username}</employeeno></DataRequest>".format(username = username)
            url = config['SF_Credentials']['end_point']
            headers = {"Authorization": config['SF_Credentials']['authorization'], "Content-Type":"text/xml"}
            try:
                # check cron_flag for ECARD
                if config['cron_flag']['ecard_flag'].lower() == "true":
                    # for ecard

                    try:
                        response = requests.post(url, data = payload, headers= headers)
                        xml_data = ET.fromstring((response.content))
                        ecard_url = xml_data.getchildren()[0].text
                    
                        # Download ECARD file
                        try:
                            ecard_file = requests.get(ecard_url)
                            
                            # for ecard
                            with open(target_path+'ecard.pdf', 'wb') as f:
                                f.write(ecard_file.content)
                                
                        except Exception as e:
                            logging.exception('error in download ecard'+ e)
                    except Exception as e:
                        pass
                # check Cron_flag for FLEX
                if config['cron_flag']['enrollment_plan'].lower() == "true":
                    try:
                        # for flex
                        response_flex = requests.post(config['SF_Credentials']['flex_end_point'], data = payload, headers= headers)
                        xml_flex = ET.fromstring((response_flex.content))    
                        flex_url = xml_flex.getchildren()[0].text         
                    
                        # download flex file
                        try:
                            flex_file = requests.get(flex_url)
                            
                            # for ecard
                            with open(target_path+'flex.pdf', 'wb') as f:
                                f.write(flex_file.content)
                        except Exception as e:
                            logging.exception('error in downloading flex '+ e)

                    except Exception:
                        pass
            except Exception as e:
                logging.exception('error in fetch sf api '+ e)
            if response.status_code >300:
                logging.exception('error in getting ecard '+ response.text)
            
            # push to azure
            self.push_to_blob(username)
            # shutil.rmtree('/home/ayush/Desktop/-ecarevad/{}'.format(username))
        except Exception as e:
            # push to azure
            self.push_to_blob(username)
            
            logging.exception('exception in getting ecard '+ e)
            
    # function for blob upload
    def push_to_blob(self, username):
            
        try:
            container_name = username.lower()
            config = read_config()
            block_blob_service = BlockBlobService(connection_string=config['azure']['connection_string'])
            
            block_blob_service.create_container(container_name)

            # Set the permission so the blobs are public.
            block_blob_service.set_container_acl(container_name, public_access=PublicAccess.Container)
        
            if config['cron_flag']['ecard_flag'].lower() == "true":
                block_blob_service.create_blob_from_path(container_name, '2019/ecard.pdf', BASE_DIR + '/{}/2019/ecard.pdf'.format(username))        
            if config['cron_flag']['enrollment_plan'].lower() == "true":
                block_blob_service.create_blob_from_path(container_name, 'flex.pdf',  BASE_DIR +'/{}/2019/flex.pdf'.format(username))   
            shutil.rmtree( BASE_DIR +'/{}'.format(username))
            
        except Exception as e:
            shutil.rmtree( BASE_DIR +'/{}'.format(username))
            logging.exception('errro in uploading blob '+ e)    
        self.count -=1

    def execute_all(self):
        # fetch usernames
        # TODO undo comment
        # self.fetch_usernames()
        
        self.count = len(self.usernames)
        # create a threadPool
        executor = ThreadPoolExecutor(max_workers=50)
        # TODO delete
        # self.usernames = self.usernames[:10]
        for username in self.usernames:
            if username[0] == 'T':
                username = username[1:]
            executor.submit(self.fetch_ecard, username)
            # self.fetch_ecard(username)
        


obj = FetchData()
# TODO delete
with open(BASE_DIR+'/users.txt', 'r') as f:
    for line in f:
        line = line[:-1]
        obj.usernames.append(line)

start_time = time.time()
obj.execute_all()

config = read_config()
print(config['azure'])
