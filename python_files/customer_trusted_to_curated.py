import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node acclelometer trusted
acclelometertrusted_node1743762070084 = glueContext.create_dynamic_frame.from_catalog(database="cc-stedi-project-db", table_name="accelerometer_trusted", transformation_ctx="acclelometertrusted_node1743762070084")

# Script generated for node customer trusted
customertrusted_node1743762039886 = glueContext.create_dynamic_frame.from_catalog(database="cc-stedi-project-db", table_name="customer_trusted", transformation_ctx="customertrusted_node1743762039886")

# Script generated for node Drop Duplicates
DropDuplicates_node1743762114911 =  DynamicFrame.fromDF(acclelometertrusted_node1743762070084.toDF().dropDuplicates(["user"]), glueContext, "DropDuplicates_node1743762114911")

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1743762277785 = ApplyMapping.apply(frame=DropDuplicates_node1743762114911, mappings=[("serialNumber", "string", "right_serialNumber", "string"), ("z", "double", "right_z", "double"), ("birthDay", "string", "right_birthDay", "string"), ("shareWithPublicAsOfDate", "long", "right_shareWithPublicAsOfDate", "long"), ("shareWithResearchAsOfDate", "long", "right_shareWithResearchAsOfDate", "long"), ("registrationDate", "long", "right_registrationDate", "long"), ("customerName", "string", "right_customerName", "string"), ("user", "string", "right_user", "string"), ("shareWithFriendsAsOfDate", "long", "right_shareWithFriendsAsOfDate", "long"), ("y", "double", "right_y", "double"), ("x", "double", "right_x", "double"), ("timestamp", "long", "right_timestamp", "long"), ("email", "string", "right_email", "string"), ("lastUpdateDate", "long", "right_lastUpdateDate", "long"), ("phone", "string", "right_phone", "string")], transformation_ctx="RenamedkeysforJoin_node1743762277785")

# Script generated for node Join
Join_node1743762159933 = Join.apply(frame1=customertrusted_node1743762039886, frame2=RenamedkeysforJoin_node1743762277785, keys1=["email"], keys2=["right_user"], transformation_ctx="Join_node1743762159933")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1743762466549 = glueContext.write_dynamic_frame.from_catalog(frame=Join_node1743762159933, database="cc-stedi-project-db", table_name="customer_curated", transformation_ctx="AWSGlueDataCatalog_node1743762466549")

job.commit()