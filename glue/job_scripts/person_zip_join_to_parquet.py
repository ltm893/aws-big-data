import sys
from datetime import date


from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

#today = date.today()
#datedir= today.strftime("%Y%m%d")
# daily_output = "s3://ltm893-gluedemo/output/" + datedir + "/people_zipgroup"


args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "input_database", "input_json_table", "input_csv_table", "output_bucket_url"]
)

#glueContext = GlueContext(SparkContext.getOrCreate())

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


persons = glueContext.create_dynamic_frame.from_catalog(
             database=args["input_database"],
             table_name=args["input_json_table"])

zip_group = glueContext.create_dynamic_frame.from_catalog(
            database=args["input_database"],
            table_name=args["input_csv_table"])

person_zip = Join.apply(persons,zip_group,'zip','zipcode').drop_fields(['zipcode'])
	   				   


glueContext.write_dynamic_frame.from_options(frame =person_zip,
          connection_type = "s3",
          connection_options = {"path": args["output_bucket_url"], "partitionKeys": ["zip"] },
          format = "parquet")

job.commit()


