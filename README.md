# Pinterest-Data-Engineering-Project
## Table of Contents
1. [Description](#description)
2. [Installation](#instruction)
3. [Structure](#structure)
    - [3.a Configuration](3.a-configuration)
    - [3.b User_posting_emulation](3.b-user_posting_emulation)

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
After creating EC2 kafka client and installing all necessaries packages on the machine, we access IAM console from our AWS account and create a user access role
to use for cluster authentication, and configure the kafka client to authenticate with the cluster by modifying the client.properties file within the "\bin kafka folder" and adding the access role created.




