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

datasource0 = glueContext.create_dynamic_frame.from_catalog(
    database = "student_data",
    table_name = "student_clean",
    transformation_ctx = "datasource0"
)

df = datasource0.toDF()

df = df.dropna()

from pyspark.sql.functions import when

df = df.withColumn(
    "performance_level",
    when(df["math score"] >= 80, "High")
    .when(df["math score"] >= 60, "Medium")
    .otherwise("Low")
)

final_df = DynamicFrame.fromDF(df, glueContext, "final_df")

glueContext.write_dynamic_frame.from_options(
    frame = final_df,
    connection_type = "s3",
    connection_options = {"path": "s3://student-performance-bucket/student_cleaned/"},
    format = "parquet"
)

job.commit()
