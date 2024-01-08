import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1687985208679 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://stedi-bucket-project3/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1687985208679",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1687985264745 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://stedi-bucket-project3/trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerTrusted_node1687985264745",
)

# Script generated for node Join
Join_node1687985384821 = Join.apply(
    frame1=StepTrainerTrusted_node1687985264745,
    frame2=AccelerometerTrusted_node1687985208679,
    keys1=["timeStamp"],
    keys2=["timeStamp"],
    transformation_ctx="Join_node1687985384821",
)

# Script generated for node Amazon S3
AmazonS3_node1687985445562 = glueContext.write_dynamic_frame.from_options(
    frame=Join_node1687985384821,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://stedi-bucket-project3/trainer/curated/",
        "partitionKeys": [],
    },
    format_options={"compression": "gzip"},
    transformation_ctx="AmazonS3_node1687985445562",
)

job.commit()
