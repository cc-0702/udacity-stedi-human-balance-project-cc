import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
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

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node customer curated
customercurated_node1743180684678 = glueContext.create_dynamic_frame.from_catalog(database="cc-stedi-project-db", table_name="customers_curated", transformation_ctx="customercurated_node1743180684678")

# Script generated for node step train trsued
steptraintrsued_node1743180737143 = glueContext.create_dynamic_frame.from_catalog(database="cc-stedi-project-db", table_name="step_trainer_trusted", transformation_ctx="steptraintrsued_node1743180737143")

# Script generated for node SQL Query
SqlQuery1923 = '''
select * from cust_curated c
left JOIN step_train s
on c.serialNumber =s.serialNumber
and c.timestamp = s.sensorReadingTime

'''
SQLQuery_node1743181251226 = sparkSqlQuery(glueContext, query = SqlQuery1923, mapping = {"cust_curated":customercurated_node1743180684678, "step_train":steptraintrsued_node1743180737143}, transformation_ctx = "SQLQuery_node1743181251226")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1743181251226, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1743181229760", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1743181648940 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1743181251226, connection_type="s3", format="json", connection_options={"path": "s3://stedi-project-cc-data/machine_learning_curated/", "compression": "snappy", "partitionKeys": []}, transformation_ctx="AmazonS3_node1743181648940")

job.commit()