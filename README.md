# Pinterest-Data-Engineering-Project
## Table of Contents
1. [Description](#description)
2. [Installation](#instruction)
3. [Structure](#structure)
    - [3.a Configuration](3.a-configuration)
    - [3.b Spark Data Cleaning and computation](3.b-spark-data-cleaning-and-computation)
    - [3.c_Orchestrating_Databricks_Workloads_on Aws_Mwaa](3.c-orchestrating-databricks-workloads-on-aws-mwaa)
   

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
#### User posting emulation :

   To send data to the API we build a python script user_posting_emulation.py file. The script will send data to the kafka topics using the API invoke URL 
   we obtained after deploying the API as shows below:
   
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
                
                Read User database credentials stored in a YAML file
        
                Parameters:
                credentials_file: YAML file containing the users credentials
        
                Return:
                ----------
                Return a key, value pair of the data securely stored in a file
        
                
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
               
     '''
   
The user_posting_emulation contain a python class with functions as describe below:

- read_database_credentials:  
  This function take as argument a yaml file containing the user database credentials,it open and the user credentials.
      
- create_db_connector:
  This function allow the user to create a connection to the database engine,it takes as argument the user credentials

- send_data:
  This function Send data collected from the database tables to the topics created with Kafka, via API Gateway and store the data
  in AWS s3 buckets.
 - post_kafka_topics:
   This function use the requests module to send data from the API to the s3 bucket.

#### Read  data from AWS to Databricks:

After data are collected and saved to AWS bucket and in order to clean and query the batch data,the data is mount from the AWS s3 bucket storing the data to Databricks.
First we create an access key and secret key to grant full acccess of the databrick account to the AWS s3 bucket and mount the S3 bucket to a specific location to Databricks as illustrate below.

     '''
     
             from pyspark.sql.functions import *
             import urllib
        
        def read_aws_keys(table_path):
            delta_table_path = table_path
            aws_df_keys = spark.read.format("delta").load(delta_table_path)
            return aws_df_keys
        
        def extract_keys(tab_path):
            aws_keys = read_aws_keys(tab_path)
            ACCESS_KEY = aws_keys.select('Access key ID').collect()[0]['Access key ID']
            print(ACCESS_KEY)
            SECRET_KEYS = aws_keys.select('Secret access key').collect()[0]['Secret access key']
            #encode secret key
            ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEYS, safe="")
            #print(ENCODED_SECRET_KEY)
            return ACCESS_KEY,ENCODED_SECRET_KEY
        
        def mount_s3_bucket(user_table_path,aws_bucket_name,mount_name):
            access_keys = extract_keys(user_table_path)
            AWS_S3_BUCKET = aws_bucket_name
            MOUNT_NAME = mount_name
            SOURCE_URL = "s3n://{0}:{1}@{2}".format(access_keys[0],access_keys[1],AWS_S3_BUCKET)
            dbutils.fs.mount(SOURCE_URL,MOUNT_NAME)
            
    '''

The read_aws_keys() function takes the delta table as argument to read user authentication credentials and, the extract_keys() extract the keys values by encoded the secret key.
The mount_s3_bucket function takes as arguments the user authentication table path, the s3 bucket name and the name of the location where the data will be mounted.
when the data is mounted to the Databricks folder, we read it using spark module by converted it to a dataframe as shows the figure below:

     '''
     
    def read_data(name,file_path,file_type):
        name = name
        print(f"{name} Dataframe")
        file_location = file_path
        file_type = file_type
        infer_schema = "true"
        df = spark.read.format(file_type)\
        .option("inferSchema",infer_schema) \
        .load(file_location)
        return df
    
     '''
The above function read_data() takes three arguments, an optional name of the dataframe, the json data folder path and the formatted file type ot the data and return the data converted to a dataframe.
The screenshots below shows the created dataframes of the data mounted from s3 bucket to the databricks account:

  - Process_data() function

    '''
               def process_data():
        
            user_delta_table_path = "dbfs:/user/hive/warehouse/authentication_credentials"
            mount_name = "/mnt/juclart_bucket_data"
            user_bucket_name = "user-0e7ae8feb921-bucket"
            pin_location = "/mnt/juclart_bucket_data/topics/0e7ae8feb921.pin/partition=0/"
            geo_location = "/mnt/juclart_bucket_data/topics/0e7ae8feb921.geo/partition=0/"
            user_location = "/mnt/juclart_bucket_data/topics/0e7ae8feb921.user/partition=0/"
            file_type = "json" 
            name =['pin','geo','user']
            
            print("Mounting Data to Datalake")
            mount_s3_bucket(user_delta_table_path,user_bucket_name,mount_name)
            
            print("CREATING DATAFRAMME")
            pin_dataframe = read_data(name[0],pin_location,file_type)
            geo_dataframe = read_data(name[1],geo_location,file_type)
            user_dataframe = read_data(name[2],user_location,file_type)
        
        
        if __name__ == "__main__":
            print("PROCESSING DATA")
            process_data()
    
    '''

  - Created dataframes after running the main script:
   - Pin_datframe:

'''
         ![image](https://github.com/juclesaint17/Pinterest-Data-Engineering-Project/assets/94936087/854a2ec7-ce00-4da1-973d-b837262d5f8a)

'''
    
   - Geo_dataframe:
     
   '''
        ![image](https://github.com/juclesaint17/Pinterest-Data-Engineering-Project/assets/94936087/cdebe7d9-77cf-4d4e-bd79-a25362f99a0a)

    '''

   - User_dataframe:
     
     '''
         ![image](https://github.com/juclesaint17/Pinterest-Data-Engineering-Project/assets/94936087/296d3baa-3c19-4ca1-ac84-89864fb7927a)

     '''
    

### 3.b Spark Data Cleaning and computation:
After loading batch data from the AWS S3 bucket and mounted it to the databricks folder,we use spark to clean data,also perform some computations to analyse the data.
the screenshots below illustrate how the data was cleaned and compute using Spark.
- Task1 : Clean the dataframe that contains information about Pinterest post

  '''
     ![image](https://github.com/user-attachments/assets/606b5870-fa41-45d8-8050-76ec7307e930)

     ![image](https://github.com/user-attachments/assets/481b42e0-cba3-48f6-b270-0544a34344f9)

     ![image](https://github.com/user-attachments/assets/85c87400-38f0-4e62-baf9-316e0fa3c625)

     ![image](https://github.com/user-attachments/assets/d98feaaa-2f70-4326-bb92-2d355e1e4261)


     ![image](https://github.com/user-attachments/assets/5589b0c9-8946-4cdf-9bb2-eb30e763064a)



  '''
  - Task 2: Clean the dataframe that contains information about Pinterest post

    '''
      ![image](https://github.com/user-attachments/assets/7e53ba7f-3300-4bc9-860d-fcb93af5d2d1)

    '''

  - Task3 : Clean the dataframe that contains information about Pinterest users
    
     '''
      ![image](https://github.com/user-attachments/assets/4bddef57-9a22-4fdb-beb6-023897f2c2e8)

     '''

  - Task 4: Most popular category in each country:

     '''
       ![image](https://github.com/user-attachments/assets/0167a40e-06bc-4bd4-a689-2df7acf3bf43)
 
       ![image](https://github.com/user-attachments/assets/0bd43690-5aea-428a-bdbf-c2745fb4de94)


     '''
  - Task 5: Most popular category each year:

     '''
       ![image](https://github.com/user-attachments/assets/cef27ddc-f26b-40ea-b5ce-3b45fa9a151d)

     '''

  -Task 6: User with most followers in each country:
  
    '''
      ![image](https://github.com/user-attachments/assets/3e132638-dab8-4411-8286-c606e4451573)

    '''
  -Task 7: Popular category for age group:

   '''
     ![image](https://github.com/user-attachments/assets/3441cca4-660e-4472-9ef2-d4b06a6cff68)

   '''
  -Task 8: Median followers count for age group:
    '''
      ![image](https://github.com/user-attachments/assets/2de1dd2e-5272-4f4b-a553-489edcac5d4f)

    '''
  - Task 9: Number of users joining each year:
    
    '''
      ![image](https://github.com/user-attachments/assets/83076f99-55b8-4ac4-a5e1-88b46cc05332)

      ![image](https://github.com/user-attachments/assets/9832d73d-1104-4399-b941-ef4dd06b2cd8)


    '''

  -Task 10: Median followers count of users based on joining year:

   '''
     ![image](https://github.com/user-attachments/assets/71873a21-955d-4442-a195-c9e45f9d4d43)

   '''

  -Task 11: Median followers count of users based on joining year and age group:

    '''
      ![image](https://github.com/user-attachments/assets/c1702de5-eda3-411b-baf5-669efaf66a04)

    '''
  
### 3.c Orchestrating Databricks Workloads on Aws Mwaa:
After performing data computations we create localy an Airflow Dag file to triggle the Databricks notebook to run on daily.
The dag wil be save on Aws bucket as illustrated below.

 '''
    ![image](https://github.com/user-attachments/assets/54ccc7b0-1064-439d-8179-be026669e240)

 '''
  
The screenshot below shows how the Airflow dag trigggle the Databricks notebook and runs the dag daily.


 '''
   ![image](https://github.com/user-attachments/assets/37c2654c-e04f-42de-bb7a-ea03e5c2d25d)

 '''




        

  
  

  




