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

# Script generated for node Customer Landing
CustomerLanding_node1743757470691 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-landing/customer_landing/"], "recurse": True}, transformation_ctx="CustomerLanding_node1743757470691")

# Script generated for node SQL Query
SqlQuery4506 = '''
select * from customer_landing WHERE shareWithResearchAsOfDate 
IS NOT NULL;

'''
SQLQuery_node1743757501891 = sparkSqlQuery(glueContext, query = SqlQuery4506, mapping = {"customer_landing":CustomerLanding_node1743757470691}, transformation_ctx = "SQLQuery_node1743757501891")

# Script generated for node CUSTOMER TRUSTED
CUSTOMERTRUSTED_node1743757558226 = glueContext.write_dynamic_frame.from_catalog(frame=SQLQuery_node1743757501891, database="cc-stedi-project-db", table_name="customer_trusted", transformation_ctx="CUSTOMERTRUSTED_node1743757558226")

job.commit()