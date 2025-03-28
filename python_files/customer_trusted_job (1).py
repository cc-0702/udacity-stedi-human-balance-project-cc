import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

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

# Script generated for node Amazon S3
AmazonS3_node1743170298458 = glueContext.create_dynamic_frame.from_catalog(database="cc-stedi-project-db", table_name="customer_landing", transformation_ctx="AmazonS3_node1743170298458")

# Script generated for node SQL Query
SqlQuery1848 = '''
select * from customer_landing
WHERE shareWithResearchAsOfDate IS NOT NULL
'''
SQLQuery_node1743170540072 = sparkSqlQuery(glueContext, query = SqlQuery1848, mapping = {"customer_landing":AmazonS3_node1743170298458}, transformation_ctx = "SQLQuery_node1743170540072")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1743176039226 = glueContext.write_dynamic_frame.from_catalog(frame=SQLQuery_node1743170540072, database="cc-stedi-project-db", table_name="customer_trusted", transformation_ctx="AWSGlueDataCatalog_node1743176039226")

job.commit()