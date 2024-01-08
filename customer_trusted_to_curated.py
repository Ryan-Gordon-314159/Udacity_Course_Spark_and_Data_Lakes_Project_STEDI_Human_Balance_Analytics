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
AccelerometerTrusted_node1687890019856 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://stedi-bucket-project3/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1687890019856",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1687889972251 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://stedi-bucket-project3/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1687889972251",
)

# Script generated for node Join
Join_node1687890086330 = Join.apply(
    frame1=CustomerTrusted_node1687889972251,
    frame2=AccelerometerTrusted_node1687890019856,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1687890086330",
)

# Script generated for node Customer Curated
CustomerCurated_node1687890447840 = glueContext.write_dynamic_frame.from_options(
    frame=Join_node1687890086330,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": "s3://stedi-bucket-project3", "partitionKeys": []},
    format_options={"compression": "gzip"},
    transformation_ctx="CustomerCurated_node1687890447840",
)

job.commit()
