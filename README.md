# Pinterest-Data-Engineering-Project
## Table of Contents
1. [Description](#description)
2. [Installation](#instruction)
3. [Structure](#structure)
    - [3.a Configuration](3.a-configuration)
   

### Description:
This project is about building a data driven application pipeline.
Pinterest data engineering project is a cloud based data driven application,we will use Amazon AWS services and Databricks
to build the data pipeline application.
The aim of this project is to access and retreive batch data from a database server,transfer the data to amazon AWS s3 buckets and mount them to Databrick in order to clean and query them using an application programming interface(API).

### Installation:
Prerequisite steps before to perform before building the application:
First we downloaded the data infractructure and, using Amazon AWS services, we create EC2 client machine and configure the instance with Apache Kafka to use it as kafka client.
After configuring the kafka instance, we install kafka package, IAM authentication package and configure IAM authentication with the kafka client to allow authentication with the cluster.

### Structure:
The structure of the project file is defined as follow:
#### 3.a Configuration:

- EC2 Client configuration:
  
  After creating EC2 kafka client and installing all necessaries packages on the machine, we access IAM console from our AWS account and create a user access role
  to use for cluster authentication, and configure the kafka client to authenticate with the cluster by modifying the client.properties file within the "\bin kafka folder" and adding      the access role created.After configuring the kafka client, we create kafka topics using the BooststrapServerString and Zookeeper connection strings retrieved from the MSK cluster.
    
- MSK cluster and s3 buckect configuration:

  This configuration will allow the data coming from the cluster to be saved automatically to the dedicated s3 bucket.
  From the kafka client machine, we download a confluent.io Amazon s3 Connector, copied it to the s3 bucket created,and access the MSK console to create a custom plugin.
  After creating the custom plugin,we create a connector and configure it with the s3 buckets name created earlier and the user role.
  The plugin-connector pair created will allow data passing through IAM cluster to be stored automatically to the designated s3 bucket.

- API in API Gateway configuration:

   To experimental the project pipeline,we build an API to send data to the cluster,which in turn will be stored in the s3 bucket created.
  We first build a kafka REst proxy integration method for the API by creating a resource,and an http ANY method,after creation we deploy the API.After deploying the API, we set up kafka REST Proxy to the kafka client machine by installing the Confluent package for Kafka Rest Proxy and allow the package to perform IAM authentication to the MSK cluster by modifying the file 'kafka-rest.properties' and start the Rest proxy as shows below:
    '''
      ![image](https://github.com/juclesaint17/Pinterest-Data-Engineering-Project/assets/94936087/beda5994-0a35-45cb-9121-7d07149b2b2b)

    '''
- Sending data configuration:

   To send data to the API we build a python script user_posting_emulation.py file. The script will send data to the kafka topics using the API invoke URL we obtained after deploying the API as shows below:
   '''
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
  '''

  '''

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
     
   
   The user_posting_emulation contain a python class with functions as describe below

     - read_database_credentials:
     
       This function take as argument a yaml file containing the user database credentials,it open and the user credentials.
      
     -create_db_connector:
     
       This function allow the user to create a connection to the database engine,it takes as argument the user credentials

     - send_data:

       This function Send data collected from the database tables to the topics created with Kafka, via API Gateway and store the data
        in AWS s3 buckets.

    - post_kafka_topics:

       This function use the requests module to send data from the API to the s3 bucket.

        

  
  

  




