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
import uuid





random.seed(100)


class AWSDBConnector:

    def __init__(self):

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
            data_stream_name:str,
            headers,
            partition_key=None
            ):
        '''
        Send data collected from the database tables to kinesis data stream folder,using API Gateway.
        
        Parameters:
        ----------
        user_credentials: User database credentials
        data: Data retrieved from the database tables
        url: API invoke Url to transit data to the AWS kinesis data stream folder
        data_stream_name: The name of the data stream created in kinesis
        headers:API headers
        '''
        
        if partition_key == None:
            partition_key = uuid.uuid4()

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

                        "StreamName":data_stream_name,

                        "records":[
                        {
                            "value":data_name
                        }
                        ],
                        "PartitionKey":partition_key
                    },default=str
                    )
                    print(json_data)
                
                    data_to_send = self.put_to_kinesis_stream(url,headers,json_data)
                    #print(data_to_send)

    
    def put_to_kinesis_stream(self,invoke_url:str,headers,data:str):        
            '''
             This function post data to kinesis streams.
             Parameters:
             -------------
             invoke_url: API url after deployement
             headers: API headers
             data: data to send to the destination

             Return:
             Return the response of the request
            '''  
            response = requests.request("PUT",invoke_url,headers=headers,data=data)
            if response.status_code == 200:
                print("Data succesfully sent")
            else:
                print(f"Response failed with status code: {response.status_code}")
                print(f"Response Text:{response.json()}")
            return response   



if __name__ == "__main__":
   
   new_connector = AWSDBConnector()

   user_credentials = 'db_creds.yaml'
   
   headers = {"Content-Type": "application/json"}

   stream_pin_url = "https://wzyu1l1xjc.execute-api.us-east-1.amazonaws.com/api_kenesis_stage/streams/streaming-0e7ae8feb921-pin/record"
   stream_geo_url = "https://wzyu1l1xjc.execute-api.us-east-1.amazonaws.com/api_kenesis_stage/streams/streaming-0e7ae8feb921-geo/record"
   stream_user_url = "https://wzyu1l1xjc.execute-api.us-east-1.amazonaws.com/api_kenesis_stage/streams/streaming-0e7ae8feb921-user/record"

   

   data_streams_pin = "streaming-0e7ae8feb921-pin"
   data_streams_geo = "streaming-0e7ae8feb921-geo"
   data_streams_user = "streaming-0e7ae8feb921-user"

   pin_data = "SELECT * FROM pinterest_data LIMIT"
   geo_data = "SELECT * FROM geolocation_data LIMIT"
   user_data = "SELECT * FROM user_data LIMIT"
   

   print("STARTING PROCESS...")

   data_pin_process = Process(target=new_connector.send_data, args=([user_credentials,pin_data,stream_pin_url,data_streams_pin,headers]))
   data_geo_process = Process(target=new_connector.send_data, args=([user_credentials,geo_data,stream_geo_url,data_streams_geo,headers]))
   data_user_process = Process(target=new_connector.send_data, args=([user_credentials,user_data,stream_user_url,data_streams_user,headers]))

   data_pin_process.start()
   data_geo_process.start()
   data_user_process.start()

   data_pin_process.join()
   data_geo_process.join()
   data_user_process.join()







    
    
    
    


