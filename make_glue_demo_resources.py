
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



print(sys.path)

logger = logging.getLogger(__name__)
logging.basicConfig(filename=f'logs/{__name__}.log', level=logging.INFO,
                    format='%(asctime)s %(message)s', datefmt='%Y-%m-%dT%H:%M:%S%z')

logger.setLevel(logging.INFO)

def setup_outfiles():
    test_data_dir = os.path.join(os.getcwd(), "testdata")
    print(test_data_dir)
    if not os.path.exists(test_data_dir):
        os.makedirs(test_data_dir)
 
    return [os.path.join(test_data_dir, 'people.json.gz') , os.path.join(test_data_dir, 'zip_group.csv.gz') ]
    



if __name__ == '__main__':
    s3_resource =  boto3.resource("s3")
    cf_client = boto3.client('cloudformation')
    bucket_name = 'ltm893-gluedemo'
    bucket_uri = 's3://' + bucket_name
    stack_name = 'gluedemo'
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
            print(f"Creating {stack_name}.")
            cf_template = os.path.join(os.getcwd(), "cloudformation","templates","create_glue_role_bucket.yaml")
            create_stack(cf_client,stack_name,cf_template)
            [ people_filenname, zg_filename ] = setup_outfiles()
            print("Making person objects")
            make_person_json(people_filenname)

            bucket = s3_resource.Bucket(bucket_name)

            print("Uploading %s to s3://%s" % (people_filenname , bucket_name))
            upload_to_bucket(bucket,people_filenname,'test_people.json.gz', s3_resource)

            print("Making zip csv file")
            make_zip_csv(zg_filename)

            #print("Uploading %s to s3://%s" % (zg_filename, bucket_name) )
            #upload_to_bucket(bucket,zg_filename ,'test_zip_group.gz', s3_resource)
                            
        elif args.action == "destroy":
            print("Emptying Bucket",bucket_name)
            bucket = s3_resource.Bucket(bucket_name)
            bucket.objects.delete()
            print(f"Deleting {stack_name}.")
            delete_stack(cf_client,stack_name)

        elif args.action == "crawler":
            try:
                print("Creating Crawler")
        
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
                    'ltm893-crawler-script',
                    "ltm893-crawler-job",
                )
            except Exception:
                logging.exception("Something went wrong Crawler")
    
    except ClientError as err:
        print(f"Something went wrong while trying to {args.action} the stack:")
        print(f"{err.response['Error']['Code']}: {err.response['Error']['Message']}")

    logger.info('End Create Objects')

    