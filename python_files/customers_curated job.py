import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node accel landing
accellanding_node1743178311419 = glueContext.create_dynamic_frame.from_catalog(database="cc-stedi-project-db", table_name="accelerometer_landing", transformation_ctx="accellanding_node1743178311419")

# Script generated for node customer trusted
customertrusted_node1743177957384 = glueContext.create_dynamic_frame.from_catalog(database="cc-stedi-project-db", table_name="customer_trusted", transformation_ctx="customertrusted_node1743177957384")

# Script generated for node Join
Join_node1743178279517 = Join.apply(frame1=customertrusted_node1743177957384, frame2=accellanding_node1743178311419, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1743178279517")

# Script generated for node SQL Query
SqlQuery1925 = '''
select * from cust_acc_join
where shareWithResearchAsOfDate is not null

'''
SQLQuery_node1743178364604 = sparkSqlQuery(glueContext, query = SqlQuery1925, mapping = {"cust_acc_join":Join_node1743178279517}, transformation_ctx = "SQLQuery_node1743178364604")

# Script generated for node Drop Duplicates
DropDuplicates_node1743178617801 =  DynamicFrame.fromDF(SQLQuery_node1743178364604.toDF().dropDuplicates(["serialNumber"]), glueContext, "DropDuplicates_node1743178617801")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1743178766998 = glueContext.write_dynamic_frame.from_catalog(frame=DropDuplicates_node1743178617801, database="cc-stedi-project-db", table_name="customers_curated", transformation_ctx="AWSGlueDataCatalog_node1743178766998")

job.commit()