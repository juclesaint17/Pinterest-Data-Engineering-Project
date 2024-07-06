import requests
import random
import boto3
import json
import yaml
import time
import sqlalchemy
from time import sleep
from sqlalchemy import text
from datetime import time
from multiprocessing import Process





random.seed(100)


class AWSDBConnector:

    def __init__(self):

        #self.HOST = "pinterestdbreadonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
        #self.USER = 'project_user'
        #self.PASSWORD = ':t%;yCY3Yjg'
        #self.DATABASE = 'pinterest_data'
        self.authentication = ''

    def read_database_credentials(self,credentials_file)-> dict:
        '''
        Read User database credentials stored in a YAML file

        Parameters:
        credentials_file: YAML file containing the users credentials

        Return:
        ----------
        Return a key, value pair of the data securely stored in a file

        '''
        try:
            print("\tLOADING CREDENTIALS...")
            #opening file containing credentials
            with open(credentials_file, 'r') as user_access:
                authentication = yaml.safe_load(user_access)
            if True:
                print("\tCREDENTIALS SUCCESSFULLY LOADED")

            return authentication
        except Exception as error:
            print("PLEASE CHECK YOUR CREDENTIALS OR CONTACT ADMIN\n", error)




    def create_db_connector(self,db_credentials:str):
        '''
        Create a database connection with user database access codes

        Parameters:
        ------------
        db_credentials: User database credentials to access  the database

        Return:
        Return a new database connection

        '''
        print("CREATING A NEW DATABASE CONNECTION...")
        print("READING USER CREDENTIALS")

        user_db_credentials = self.read_database_credentials(db_credentials)

        #creating an empty list to store the credentials
        credentials_list =[]

        #accessing credentials file to collect the credentials values
        credentials_values = list(user_db_credentials.values())

        # appending credentials value to the created list
        for data in credentials_values:
            credentials_list.append(data)
        
        HOST = credentials_list[0]
        USER = credentials_list[1]
        PASSWORD = credentials_list[2]
        DATABASE = credentials_list[3]
        PORT = credentials_list[4]
        
        try:
            print("\tINITIATING DATABASE ENGINE CONNECTION..")

            engine = sqlalchemy.create_engine(f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}?charset=utf8mb4")
            if True:
                
                print("DATABASE ENGINE SUCCESSEFULLY INITIATED")
            
                print(f"\tUSER: {USER} CONNECTED TO THE DATABASE SERVER")
            return engine
        except Exception as error:
            print("DATABASE ENGINE INITIATION FAILED",error)


    def send_data(
            self,
            user_credentials:str,
            data:str,
            url:str,
            headers
            ):
        '''
        Send data collected from the database tables to the topics created with Kafka, via API Gateway and store the data
        in AWS s3 buckets.
        
        Parameters:
        ----------
        user_credentials: User database credentials
        data: Data retrieved from the database tables
        url: API invoke Url to transit data to the AWS s3 buckets
        headers:API headers
        '''

        while True:
            sleep(random.randrange(0, 2))
            random_row = random.randint(0, 11000)
            engine = self.create_db_connector(user_credentials)
            
            with engine.connect() as connection:

                data_string = text(f"{data} {random_row}, 1")
                data_selected_row = connection.execute(data_string)
                    
                for row in data_selected_row:

                    data_name = dict(row._mapping)
                    json_data = json.dumps({
                        "records":[
                        {
                            "value":data_name
                        }
                        ]
                    },default=str
                    )
                    data_to_send = self.post_to_kafka_topics(url,headers,json_data)

    
    def post_to_kafka_topics(self,invoke_url:str,headers,data:str):
            
            response = requests.request("POST",invoke_url,headers=headers,data=data)
            if response.status_code == 200:
                print("Data succesfully sent")
            else:
                print(f"Response failed with status code: {response.status_code}")
                print(f"Response Text:{response.json()}")
            return response   



if __name__ == "__main__":
   
   new_connector = AWSDBConnector()

   user_credentials = 'db_creds.yaml'
   
   headers = {"Content-Type": "application/vnd.kafka.json.v2+json"}

   pin_url = "https://wzyu1l1xjc.execute-api.us-east-1.amazonaws.com/second_stage/topics/0e7ae8feb921.pin"
   geo_url = "https://wzyu1l1xjc.execute-api.us-east-1.amazonaws.com/second_stage/topics/0e7ae8feb921.geo"
   user_url = "https://wzyu1l1xjc.execute-api.us-east-1.amazonaws.com/second_stage/topics/0e7ae8feb921.user"

   pin_data = "SELECT * FROM pinterest_data LIMIT"
   geo_data = "SELECT * FROM geolocation_data LIMIT"
   user_data = "SELECT * FROM user_data LIMIT"
   
   print("STARTING PROCESS...")

   data_pin_process = Process(target=new_connector.send_data, args=([user_credentials,pin_data,pin_url,headers]))
   data_geo_process = Process(target=new_connector.send_data, args=([user_credentials,geo_data,geo_url,headers]))
   data_user_process = Process(target=new_connector.send_data, args=(user_credentials,user_data,user_url,headers))

   data_pin_process.start()
   data_geo_process.start()
   data_user_process.start()

   data_pin_process.join()
   data_geo_process.join()
   data_user_process.join







    
    
    
    


