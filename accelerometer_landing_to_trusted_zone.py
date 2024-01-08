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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1687800248912 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-bucket-project3/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLanding_node1687800248912",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1687800447184 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://stedi-bucket-project3/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1687800447184",
)

# Script generated for node Join
Join_node1687800593009 = Join.apply(
    frame1=AccelerometerLanding_node1687800248912,
    frame2=CustomerTrusted_node1687800447184,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1687800593009",
)

# Script generated for node DropFields
DropFields_node1687800827215 = DropFields.apply(
    frame=Join_node1687800593009,
    paths=[
        "serialNumber",
        "birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "customerName",
        "shareWithFriendsAsOfDate",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithPublicAsOfDate",
    ],
    transformation_ctx="DropFields_node1687800827215",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1687801567377 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1687800827215,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://stedi-bucket-project3/accelerometer/trusted/",
        "partitionKeys": [],
    },
    format_options={"compression": "gzip"},
    transformation_ctx="AccelerometerTrusted_node1687801567377",
)

job.commit()
