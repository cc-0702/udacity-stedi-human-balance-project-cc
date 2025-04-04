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

# Script generated for node accelerometer_landing
accelerometer_landing_node1743758248423 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-landing/accelerometer_landing/"], "recurse": True}, transformation_ctx="accelerometer_landing_node1743758248423")

# Script generated for node customer_trusted
customer_trusted_node1743758283593 = glueContext.create_dynamic_frame.from_catalog(database="cc-stedi-project-db", table_name="customer_trusted", transformation_ctx="customer_trusted_node1743758283593")

# Script generated for node Join
Join_node1743758322676 = Join.apply(frame1=accelerometer_landing_node1743758248423, frame2=customer_trusted_node1743758283593, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1743758322676")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1743758362642 = glueContext.write_dynamic_frame.from_catalog(frame=Join_node1743758322676, database="cc-stedi-project-db", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1743758362642")

job.commit()