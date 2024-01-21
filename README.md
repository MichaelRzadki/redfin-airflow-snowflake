# Redfin Data Engineering with Airflow and Snowflake


## Overview
This project utilizes Apache Airflow on an EC2 instance to extract and transform real estate data from Redfin, storing it in an S3 bucket, and then loading it into Snowflake as a data warehouse. Finally, Power BI is employed to visualize the extracted and transformed data through a dashboard.

## Project Goals
1. Data Ingestion - Build a mechanism to ingest data from different sources.
2. ETL System: Transform raw data into the proper format.
3. Data Lake: Create a centralized repository (S3 Bucket) and Snowflake to store data.
4. Scalability: Ensure the system scales with increasing data size.
5. Cloud Compute: Utilize AWS EC2 and Airflow for processing vast amounts of data.
6. Reporting: Create a Power BI dashboard to visualize the data

## Services Used
### 1. EC2 (Elastic Compute Cloud)
  - Utilized as a hosting environment for running Apache Airflow, managing the orchestration of data extraction, transformation, and loading processes..
### 2. Airflow
  - Acted as the workflow orchestrator, enabling the creation, scheduling, and monitoring of data engineering tasks on the EC2 instance, facilitating a seamless data pipeline.
### 3. Amazon S3 (Simple Storage Service):
  - Served as storage for both raw and transformed real estate data, offering scalable and durable cloud storage, pivotal in the data engineering process.
### 4. Snowflake
  - Functioned as the data warehouse where the transformed data was loaded, providing a scalable and efficient platform for storing and querying structured data.
### 5. Power BI
  -  Used for data visualization, creating interactive dashboards to represent insights derived from the transformed data in Snowflake, enhancing the accessibility and interpretation of the project's outcomes.

## Pre-Rec Setup
1. Create [AWS Account](https://repost.aws/knowledge-center/create-and-activate-aws-account) - Set default AZ to North Virginia (us-east-1).
2. Create [Snowflake Account](https://signup.snowflake.com/?utm_source=google&utm_medium=paidsearch&utm_campaign=na-ca-en-brand-cloud-phrase&utm_content=go-rsa-evg-ss-free-trial&utm_term=c-g-snowflake%20computing-p&_bt=586482091113&_bk=snowflake%20computing&_bm=p&_bn=g&_bg=136172936548&gclsrc=aw.ds&gad_source=1&gclid=Cj0KCQiAnrOtBhDIARIsAFsSe508RUX5iE7-rE9zmkOiiMj8XrBDwdUUYsjfDTCi3Rq_0hcQlomJa0QaAkMqEALw_wcB).
3. Download and Set Up [AWS CLI](https://aws.amazon.com/cli/): Install AWS CLI and configure it. 
4. Find the appropriate Redfin Data - Download the [Redfin City level data](https://www.redfin.com/news/data-center/).

## Part 1: Create IAM Account for Project
  - Create an EC2 instance (t2.xlarge)
  - Generate an RSA keypair and download the .pem file (Name: redfinsnowflake).

## Part 2: Add Command Requirements
  - Connect to the EC2 instance and install necessary dependencies from the AWS_EC2_CLI_Commands.sh file.

## Part 3: Configure Security Group
  - Edit the Security Group attached to the EC2 instance and add an inbound rule to open port 8080.

## Part 4: Setup Airflow and DAG
  - Active EC2 instance and update inbound rules to open port 8080.
  - Login to Airflow by visiting http://<your_ec2_instance_ip>:8080/login/ (username: admin, password: **************).
  - Set up SSH into VS Code and create a "dags" folder within the airflow folder.
  - Create a redfin_analysis.py file containing DAG tasks for extracting, transforming, and loading data.

## Part 5: Setup S3 Buckets
  - Create 2 S3 buckets, one for storing raw and one for the transformed data.

## Part 6: Run Airflow DAG
  - Run the DAG script in Airflow to initiate the data pipeline.

## Part 8: Setup Snowflake
  - Create a Snowflake account and set up a new SQL workbook.
  - Run the SQL command DESC PIPE redfin_realestate_database.snowpipe_schema.redfin_snowpipe; and note the notification_channel value.

## Part 9: Add Event Triggers
  - Add an event trigger in the transformed S3 data bucket with destination type SQS queue, destination ARN, and event type as All object create events.
  - Rerun the DAG trigger in Airflow and check for results.

## Part 10: Power BI Visualization
  - Login to Power BI and connect to the data source from Snowflake.
  - Create a visualization dashboard to display the extracted and transformed real estate data.

## Final Output Dashboard
<img src="Power Bi Dashboard.png">
