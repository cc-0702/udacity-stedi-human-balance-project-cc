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

# Script generated for node step trainer landing
steptrainerlanding_node1743762677780 = glueContext.create_dynamic_frame.from_catalog(database="cc-stedi-project-db", table_name="step_trainer_landing", transformation_ctx="steptrainerlanding_node1743762677780")

# Script generated for node customer curated
customercurated_node1743762703467 = glueContext.create_dynamic_frame.from_catalog(database="cc-stedi-project-db", table_name="customer_curated", transformation_ctx="customercurated_node1743762703467")

# Script generated for node Drop Duplicates
DropDuplicates_node1743762745032 =  DynamicFrame.fromDF(customercurated_node1743762703467.toDF().dropDuplicates(["serialnumber"]), glueContext, "DropDuplicates_node1743762745032")

# Script generated for node SQL Query
SqlQuery4838 = '''
SELECT * 
FROM step_trainer_landing a
JOIN customer_curated b 
ON a.serialnumber = b.serialnumber;
'''
SQLQuery_node1743763404490 = sparkSqlQuery(glueContext, query = SqlQuery4838, mapping = {"step_trainer_landing":steptrainerlanding_node1743762677780, "customer_curated":DropDuplicates_node1743762745032}, transformation_ctx = "SQLQuery_node1743763404490")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1743763490021 = glueContext.write_dynamic_frame.from_catalog(frame=SQLQuery_node1743763404490, database="cc-stedi-project-db", table_name="step_trainer_trusted", transformation_ctx="AWSGlueDataCatalog_node1743763490021")

job.commit()