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
  

  




