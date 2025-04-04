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

# Script generated for node accelerometer trusted
accelerometertrusted_node1743764386141 = glueContext.create_dynamic_frame.from_catalog(database="cc-stedi-project-db", table_name="accelerometer_trusted", transformation_ctx="accelerometertrusted_node1743764386141")

# Script generated for node step trainer trusted
steptrainertrusted_node1743764360176 = glueContext.create_dynamic_frame.from_catalog(database="cc-stedi-project-db", table_name="step_trainer_trusted", transformation_ctx="steptrainertrusted_node1743764360176")

# Script generated for node SQL Query
SqlQuery4494 = '''
select user, timestamp, x,y,z,sensorreadingtime,s.serialnumber,distancefromobject
from step_trainer_trusted s
left JOIN accelerometer_trusted a
on s.sensorreadingtime = a.timestamp
'''
SQLQuery_node1743764588248 = sparkSqlQuery(glueContext, query = SqlQuery4494, mapping = {"step_trainer_trusted":steptrainertrusted_node1743764360176, "accelerometer_trusted":accelerometertrusted_node1743764386141}, transformation_ctx = "SQLQuery_node1743764588248")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1743764952081 = glueContext.write_dynamic_frame.from_catalog(frame=SQLQuery_node1743764588248, database="cc-stedi-project-db", table_name="machine_learning_curated", transformation_ctx="AWSGlueDataCatalog_node1743764952081")

job.commit()