## Udacity Stedi Human Balance Project

# Project details

As part of my Stedi Human Balance Project I have been tasked with setting up and running ETL pipelines to process STEDI Step Trainer data within AWS. The data will eventually be used to train a machine learning model to accutately detect steps in real-time. 


# Project data

The original datasets consists of JSON files stored in s3:
- accelerometer data: Data from the mobile app containing accelerometer information.
- customer data: Customer information from the STEDI website.
- step trainer data: Motion sensor data from the actual step trainer device. 


The "lakehouse" architecture consists of three zones:
- Landing Zone: this stores the raw data from the source (see above)
- Trusted Zone: the cleaned and validated data.
- Curated Zone: Contains the transformed data that will be used to train the machine learning model.

# Data journey and structure 

1. Set-up three s3 buckets for the landing zones using the JSON files:
- s3://stedi-lake-landing/accelerometer_landing/
- s3://stedi-lake-landing/customer_landing/
- s3://stedi-lake-landing/step_trainer_landing/


2. Utilise AWS Glue/glue studio to create the tables from the landing zone. This means we can query the data in Athena and see the metadata in AWS glue.
  
3. Created ETL scripts with glue. I used ETL visual/pyspark to create the scripts.

Landing zone to customer trusted with the following cleaning/validating:

- Filter customer records to include only those who are happy to share their data. 

- Join accelerometer data with the trusted customer data.

- Join step trainer data with the trusted customer data. 

Trusted to Curated:

- Further transform the data to prepare it for the machine learning model. For example, joining step trainer and accelerometer on timestamps.

  All tables where queried with Athena (and screen shots added).





