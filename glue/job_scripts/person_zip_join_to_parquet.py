import sys
from datetime import date


from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

today = date.today()
datedir= today.strftime("%Y%m%d")
daily_output = "s3://ltm893-gluedemo/output/" + datedir + "/people_zipgroup"

glueContext = GlueContext(SparkContext.getOrCreate())


persons = glueContext.create_dynamic_frame.from_catalog(
             database="ltm893-crawler-demo-database",
             table_name="ltm893-crawler-json")

zip_group = glueContext.create_dynamic_frame.from_catalog(
            database="ltm893-crawler-demo-database",
            table_name="ltm893-crawler-csv")

person_zip = Join.apply(persons,zip_group,'zip','zipcode').drop_fields(['zipcode'])				   				   


glueContext.write_dynamic_frame.from_options(frame =person_zip,
          connection_type = "s3",
          connection_options = {"path": daily_output },
          format = "parquet")


