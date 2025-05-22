import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Glue setup
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Lee los datos desde el catálogo (crawler)
datasource0 = glueContext.create_dynamic_frame.from_catalog(
    database = "student_data",
    table_name = "student_clean",  # el nombre real que generó tu crawler
    transformation_ctx = "datasource0"
)

# Transforma a DataFrame de Spark
df = datasource0.toDF()

# 💧 Limpieza de datos
df = df.dropna()

# 🎯 Clasifica a los estudiantes según su nota final
from pyspark.sql.functions import when

df = df.withColumn(
    "performance_level",
    when(df["math score"] >= 80, "High")
    .when(df["math score"] >= 60, "Medium")
    .otherwise("Low")
)

# Convierte de nuevo a DynamicFrame para escribir
final_df = DynamicFrame.fromDF(df, glueContext, "final_df")

# Guarda en S3 como archivo limpio (Parquet recomendado)
glueContext.write_dynamic_frame.from_options(
    frame = final_df,
    connection_type = "s3",
    connection_options = {"path": "s3://student-performance-bucket/student_cleaned/"},
    format = "parquet"
)

job.commit()
