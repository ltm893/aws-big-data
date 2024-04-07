from datetime import date
import argparse
import os
import boto3
import logging
from botocore.exceptions import ClientError
import sys

for dir in ["cloudformation","make_people","glue"] : 
    sys.path.append(os.path.join(os.getcwd(), dir))

from cloudformation.stacks import *
from make_people.create_objects import *
from glue.crawler import *

from glue.question import Question

logger = logging.getLogger(__name__)
#logging.basicConfig(filename=f'logs/{__name__}.log', level=logging.INFO,
 #                   format='%(asctime)s %(message)s', datefmt='%Y-%m-%dT%H:%M:%S%z')
logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
logger = logging.getLogger(__name__)

fileHandler = logging.FileHandler("{0}/{1}.log".format('logs','{__name__}.log' ))
fileHandler.setFormatter(logFormatter)
logger.addHandler(fileHandler)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
logger.addHandler(consoleHandler)

logger.setLevel(logging.INFO)

def setup_outfiles():
    test_data_dir = os.path.join(os.getcwd(), "testdata")
    print(test_data_dir)
    if not os.path.exists(test_data_dir):
        os.makedirs(test_data_dir)
 
    return [os.path.join(test_data_dir, 'people.json.gz') , os.path.join(test_data_dir, 'zip_group.csv.gz') ]

def remove_bucket_output_dated(s3_resource,date):
    prefix = 'output/' + date 
    response = s3_resource.meta.client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    files_in_folder = response["Contents"]
    files_to_delete = []
    print(f"deleting output directory for {date}")
    for f in files_in_folder:
        files_to_delete.append({"Key": f["Key"]})

    response = s3_resource.meta.client.delete_objects(
        Bucket=bucket_name, Delete={"Objects": files_to_delete}
    )
   
  



if __name__ == '__main__':


    s3_resource =  boto3.resource("s3")
    cf_client = boto3.client('cloudformation')
    bucket_name = 'ltm893-gluedemo'
    bucket_uri = 's3://' + bucket_name
    bucket_output = bucket_uri + '/output'
    stack_name = 'gluedemo'
    today = date.today()
    datedir= today.strftime("%Y%m%d")
    bucket_output_dated = bucket_output + '/' + datedir

    person_zip_join_script_name = 'person_zip_join_to_parquet.py'
    local_person_zip_join_script = os.path.join('glue','job_scripts',person_zip_join_script_name)
    s3_prefix_job_script = 'job_scripts/' + person_zip_join_script_name

    logger.info('Beginging run')     

    parser = argparse.ArgumentParser(
        description="Deploys and destroys s3 Bucket, IAM Role and Test Data"
    )
    parser.add_argument(
        "action",
        choices=["deploy", "destroy", "crawler"],
        help="Indicates the action that the script performs.",
    )

    args = parser.parse_args()
  

    try:
        if args.action == "deploy":

            ready = Question.ask_question(
                "Ready to start the crawler? (y/n) ", Question.is_yesno
            )

            print(f"Creating {stack_name} Stack.")
            cf_template = os.path.join(os.getcwd(), "cloudformation","templates","create_glue_role_bucket.yaml")
            create_stack(cf_client,stack_name,cf_template)
            [ people_filenname, zg_filename ] = setup_outfiles()
            print("Making person objects")
            make_person_json(people_filenname)

            bucket = s3_resource.Bucket(bucket_name)

            print("Uploading %s to s3://%s" % (people_filenname , bucket_name))
            upload_to_bucket(bucket,people_filenname,'json/test_people.json.gz', s3_resource)

            print("Making zip csv file")
            make_zip_csv(zg_filename)

            print("Uploading %s to s3://%s" % (zg_filename, bucket_name) )
            upload_to_bucket(bucket,zg_filename ,'csv/test_zip_group.gz', s3_resource)

            print("Uploading %s to %s" % (local_person_zip_join_script,bucket_uri + '/job_scripts' ) )
            upload_to_bucket(bucket,local_person_zip_join_script, s3_prefix_job_script,s3_resource)  

        elif args.action == "destroy":
            print("Emptying Bucket",bucket_name)
            bucket = s3_resource.Bucket(bucket_name)
            bucket.objects.delete()
            print(f"Deleting {stack_name} Stack.")
            delete_stack(cf_client,stack_name)

        elif args.action == "crawler":
           
            remove_bucket_output_dated(s3_resource,datedir)
            print("Creating Crawler Boto3 Client")
            
            scenario = GlueCrawler(
                boto3.client("glue"),
                boto3.resource("iam").Role('DemoGlueServiceRole1'),
                boto3.resource("s3").Bucket(bucket_name),
            )
            # scenario.upload_job_script()
            scenario.run(
                "ltm893-crawler-demo",
                "ltm893-crawler-demo-database",
                "ltm893-crawler-",
                bucket_uri ,
                bucket_uri + '/'  + s3_prefix_job_script ,
                "ltm893-crawler-job",
            )

    
    except ClientError as err:
        print(f"Something went wrong while trying to {args.action} the stack:")
        print(f"{err.response['Error']['Code']}: {err.response['Error']['Message']}")

    logger.info('End Create Objects')

    