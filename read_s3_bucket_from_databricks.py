# Databricks notebook source
from pyspark.sql.functions import *
import urllib
import dbutils


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

