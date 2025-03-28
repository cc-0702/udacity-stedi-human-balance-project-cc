import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node step train landing
steptrainlanding_node1743179241164 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-project-cc-data/step_trainer_landing/"], "recurse": True}, transformation_ctx="steptrainlanding_node1743179241164")

# Script generated for node customers curated
customerscurated_node1743179072778 = glueContext.create_dynamic_frame.from_catalog(database="cc-stedi-project-db", table_name="customers_curated", transformation_ctx="customerscurated_node1743179072778")

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1743179307962 = ApplyMapping.apply(frame=steptrainlanding_node1743179241164, mappings=[("sensorreadingtime", "long", "right_sensorreadingtime", "long"), ("serialnumber", "string", "right_serialnumber", "string"), ("distancefromobject", "int", "right_distancefromobject", "int")], transformation_ctx="RenamedkeysforJoin_node1743179307962")

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1743179404029 = ApplyMapping.apply(frame=customerscurated_node1743179072778, mappings=[("serialnumber", "string", "right_serialnumber", "string"), ("z", "double", "right_z", "double"), ("birthday", "string", "right_birthday", "string"), ("sharewithpublicasofdate", "long", "right_sharewithpublicasofdate", "long"), ("sharewithresearchasofdate", "long", "right_sharewithresearchasofdate", "long"), ("registrationdate", "long", "right_registrationdate", "long"), ("customername", "string", "right_customername", "string"), ("user", "string", "right_user", "string"), ("y", "double", "right_y", "double"), ("sharewithfriendsasofdate", "long", "right_sharewithfriendsasofdate", "long"), ("x", "double", "right_x", "double"), ("timestamp", "long", "right_timestamp", "long"), ("email", "string", "right_email", "string"), ("lastupdatedate", "long", "right_lastupdatedate", "long"), ("phone", "string", "right_phone", "string")], transformation_ctx="RenamedkeysforJoin_node1743179404029")

# Script generated for node Join
steptrainlanding_node1743179241164DF = steptrainlanding_node1743179241164.toDF()
RenamedkeysforJoin_node1743179404029DF = RenamedkeysforJoin_node1743179404029.toDF()
Join_node1743179125348 = DynamicFrame.fromDF(steptrainlanding_node1743179241164DF.join(RenamedkeysforJoin_node1743179404029DF, (steptrainlanding_node1743179241164DF['serialnumber'] == RenamedkeysforJoin_node1743179404029DF['right_serialnumber']), "left"), glueContext, "Join_node1743179125348")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1743179438002 = glueContext.write_dynamic_frame.from_catalog(frame=Join_node1743179125348, database="cc-stedi-project-db", table_name="step_trainer_trusted", transformation_ctx="AWSGlueDataCatalog_node1743179438002")

job.commit()