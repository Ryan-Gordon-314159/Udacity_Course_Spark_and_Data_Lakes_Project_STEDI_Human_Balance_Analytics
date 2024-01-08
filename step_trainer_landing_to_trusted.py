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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1687953887017 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-bucket-project3/trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1687953887017",
)

# Script generated for node Customer Curated
CustomerCurated_node1687953842646 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://stedi-bucket-project3/customer/curated"],
        "recurse": True,
    },
    transformation_ctx="CustomerCurated_node1687953842646",
)

# Script generated for node Join
Join_node1687954051380 = Join.apply(
    frame1=CustomerCurated_node1687953842646,
    frame2=StepTrainerLanding_node1687953887017,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="Join_node1687954051380",
)

# Script generated for node Amazon S3
AmazonS3_node1687954171001 = glueContext.write_dynamic_frame.from_options(
    frame=Join_node1687954051380,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://stedi-bucket-project3/trainer/trusted/",
        "partitionKeys": [],
    },
    format_options={"compression": "gzip"},
    transformation_ctx="AmazonS3_node1687954171001",
)

job.commit()
