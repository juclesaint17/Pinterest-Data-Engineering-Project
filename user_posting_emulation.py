import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text
from datetime import time


random.seed(100)


class AWSDBConnector:

    def __init__(self):

        self.HOST = "pinterestdbreadonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
        self.USER = 'project_user'
        self.PASSWORD = ':t%;yCY3Yjg'
        self.DATABASE = 'pinterest_data'
        self.PORT = 3306
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


    def send_data(self,data:str,url:str,headers):

        while True:
            sleep(random.randrange(0, 2))
            random_row = random.randint(0, 11000)
            engine = self.create_db_connector()
            
            with engine.connect() as connection:

                data_string = text(f"{data} {random_row}, 1")
                data_selected_row = connection.execute(data_string)
                    
                for row in data_selected_row:
                    data_name = dict(row._mapping)
                    data_to_send = self.post_to_kafka_topics(url,headers,data_name)
                print(data_name)
                #return data_to_send
    
    def post_to_kafka_topics(self,invoke_url:str, headers,data:str):
            
            response = requests.request("POST",url=invoke_url,headers=headers,data=data)
            if response.status_code == 200:
                print("Data succesfully sent")
            else:
                print(f"Response failed with status code: {response.status_code}")
                print(f"Response Text:{response.text}")
            return response   



if __name__ == "__main__":
   
   headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}

   pin_url = "https://wzyu1l1xjc.execute-api.us-east-1.amazonaws.com/second_stage/topics/0e7ae8feb921.pin"
   geo_url = "https://wzyu1l1xjc.execute-api.us-east-1.amazonaws.com/second_stage/topics/0e7ae8feb921.geo"
   user_url = "https://wzyu1l1xjc.execute-api.us-east-1.amazonaws.com/second_stage/topics/0e7ae8feb921.user"


   pin_data = "SELECT * FROM pinterest_data LIMIT"
   geo_data = "SELECT * FROM geolocation_data LIMIT"
   user_data = "SELECT * FROM user_data LIMIT"

   new_connector = AWSDBConnector()

   print("----------SENDING PIN DATA")
   new_connector.send_data(pin_data,pin_url,headers)
   print("----------SENDING GEO DATA")
   new_connector.send_data(data=geo_data,url=geo_url,headers=headers)
   print("----------SENDING USER DATA")
   new_connector.send_data(data=user_data,url=user_url,headers=headers)








    
    
    
    


