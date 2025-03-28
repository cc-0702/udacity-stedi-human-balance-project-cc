import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node accelerometer landing
accelerometerlanding_node1743176280124 = glueContext.create_dynamic_frame.from_catalog(database="cc-stedi-project-db", table_name="accelerometer_landing", transformation_ctx="accelerometerlanding_node1743176280124")

# Script generated for node customer trusted
customertrusted_node1743176326657 = glueContext.create_dynamic_frame.from_catalog(database="cc-stedi-project-db", table_name="customer_trusted", transformation_ctx="customertrusted_node1743176326657")

# Script generated for node Join
Join_node1743176359899 = Join.apply(frame1=accelerometerlanding_node1743176280124, frame2=customertrusted_node1743176326657, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1743176359899")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1743176522139 = glueContext.write_dynamic_frame.from_catalog(frame=Join_node1743176359899, database="cc-stedi-project-db", table_name="accelerometer_trusted", transformation_ctx="AWSGlueDataCatalog_node1743176522139")

job.commit()