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

today = date.today()
DATE_STRING = today.strftime("%Y%m%d")
DEMO_TYPE = 'gluedemo'

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




class GlueDemo(): 
    def __init__(self, base_name):
        self.name = f'{base_name}-{DEMO_TYPE}'
        self.stack_name = f'{self.name}-stack'
        self.stack_template = os.path.join(os.getcwd(), "cloudformation","templates","create_glue_role_bucket.yaml")
        self.glue_db_name = f'{base_name}-{DEMO_TYPE}-database'
        self.bucket_name  = f'{DEMO_TYPE}-{DATE_STRING}-{base_name}-{GlueDemo.__make_rs(6)}'

        self.bucket_uri = f's3://{self.bucket_name}'
        self.bucket_output_uri = f'{self.bucket_uri}/output'
        self.bucket_output_dated_uri = f'{self.bucket_output_uri}/{DATE_STRING}'
        self.glue_run_job_path = os.path.join('glue','job_scripts')
        self.glue_run_script = 'person_zip_join_to_parquet.py'
        self.s3_job_prefix   = f'{self.bucket_uri}/script'
        self.test_data_dir = os.path.join(os.getcwd(), "testdata")
        self.person_test_file = f'{self.test_data_dir}/people.json.gz' 
        self.zip_test_file = f'{self.test_data_dir}/zip_group.csv.gz'

 
    def __make_rs(num):
        chars = string.ascii_lowercase + string.digits
        return ''.join(random.choice(chars) for _ in range(num) )


def make_demo_config_from_user() :  
    while True :
        base_name = input("Please enter an alpha numeric friendly name less than 20 characters for base name: ")
        if base_name.isalnum() and len(base_name) <= 20 :
            config = GlueDemo(base_name)
            pprint(config.__dict__)
            create_stack = input('Creating stack with above these details y/n: ')
            if (create_stack == 'y'):
                return config
        else :
            print(base_name, 'base name not valid')


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
            description="Deploys and destroys s3 Bucket, IAM Role and Test Data"
        )
    parser.add_argument(
        "action",
        choices=["deploy", "destroy", "crawler"],
        help="Indicates the action that the script performs.",
    )
    s3_resource =  boto3.resource("s3")
    cf_client = boto3.client("cloudformation")

    args = parser.parse_args()
    try:
        if args.action == "deploy":
            config = make_demo_config_from_user()
            # create_stack(cf_client,config.stack_name,config.stack_template)  
            create_stack(cf_client,
                Capabilities=['CAPABILITY_NAMED_IAM'], StackName=config.stack_name, 
                TemplateBody=config.stack_template, Parameters=[{ 'ParameterKey': 'BucketName', 'ParameterValue': config.bucket_name}] )
            # bucket = s3_resource.Bucket(config.bucket_name)    
            # create_stack(cf_client,stack_name,cf_template)

            '''
            [ people_filenname, zg_filename ] = setup_outfiles()
            print("Making person objects")
            make_person_json(people_filenname)

            

           
            print("Uploading %s to s3://%s" % (people_filenname , bucket_name))
            upload_to_bucket(bucket,people_filenname,'json/test_people.json.gz', s3_resource)

            print("Making zip csv file")
            make_zip_csv(zg_filename)

            print("Uploading %s to s3://%s" % (zg_filename, bucket_name) )
            upload_to_bucket(bucket,zg_filename ,'csv/test_zip_group.gz', s3_resource)

            print("Uploading %s to %s" % (local_person_zip_join_script,bucket_uri + '/job_scripts' ) )
            upload_to_bucket(bucket,local_person_zip_join_script, s3_prefix_job_script,s3_resource)  

            
            '''
        elif args.action == "destroy":
            pass
           

        elif args.action == "crawler":
            pass

    except ClientError as err:
        print(f"Something went wrong while trying to {args.action} the stack:")
        print(f"{err.response['Error']['Code']}: {err.response['Error']['Message']}")
