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

# Script generated for node transformed-visa-data
transformedvisadata_node1712968323412 = glueContext.create_dynamic_frame.from_catalog(database="travelvisa-db", table_name="michelledata", transformation_ctx="transformedvisadata_node1712968323412")

# Script generated for node Change Schema
ChangeSchema_node1712968890923 = ApplyMapping.apply(frame=transformedvisadata_node1712968323412, mappings=[("year", "long", "year", "long"), ("regional code", "long", "regional code ID", "long"), ("country", "string", "country", "string"), ("number of issued", "long", "number of issued", "long"), ("number of issued_numerical", "long", "number of issued_numerical", "long"), ("travel certificate", "double", "travel certificate ID", "double"), ("diplomacy", "double", "diplomacy", "double"), ("public use", "double", "public use", "double"), ("passing", "long", "passing", "long"), ("short -term stay", "long", "short -term stay", "long"), ("staying in medical care", "double", "staying in medical care", "double"), ("advanced profession", "double", "advanced profession", "double"), ("employment", "long", "employment", "long"), ("employment_professor", "double", "employment_professor", "double"), ("employment_art", "double", "employment_art", "double"), ("employment_religion", "double", "employment_religion", "double"), ("employment_report", "double", "employment_report", "double"), ("employment_management / management", "double", "employment_management / management", "double"), ("employment_law accounting", "double", "employment_law accounting", "double"), ("employment_medical care", "double", "employment_medical care", "double"), ("employment_research", "double", "employment_research", "double"), ("employment_education", "double", "employment_education", "double"), ("employment_technology", "double", "employment_technology", "double"), ("employment_humanities international", "double", "employment_humanities international", "double"), ("employment_c corporate rotation", "double", "employment_c corporate rotation", "double"), ("employment_show", "long", "employment_show", "long"), ("employment_skills", "double", "employment_skills", "double"), ("ordinary", "long", "ordinary", "long"), ("general_cultural activity", "double", "general_cultural activity", "double"), ("general_studying abroad", "double", "general_studying abroad", "double"), ("general_arademy", "double", "general_arademy", "double"), ("general_training", "long", "general_training", "long"), ("general_family stay", "double", "general_family stay", "double"), ("general_stifying 1 i", "double", "general_stifying 1 i", "double"), ("general_steng 1", "double", "general_steng 1", "double"), ("identification", "long", "identification", "long"), ("specific_housework employee", "double", "specific_housework employee", "double"), ("specific_profit representative staff", "double", "specific_profit representative staff", "double"), ("specific_working holiday", "double", "specific_working holiday", "double"), ("specified_amasport", "double", "specified_amasport", "double"), ("`specific_japanese spouse, etc.`", "long", "`specific_japanese spouse, etc.`", "long"), ("`specific_permanent resident's spouse, etc.`", "double", "`specific_permanent resident's spouse, etc.`", "double"), ("specified_distingant", "long", "specified_distingant", "long"), ("specific_others", "double", "specific_others", "double")], transformation_ctx="ChangeSchema_node1712968890923")

# Script generated for node load-transformed-data
loadtransformeddata_node1712969351071 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1712968890923, connection_type="s3", format="glueparquet", connection_options={"path": "s3://michelledata", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="loadtransformeddata_node1712969351071")

job.commit()