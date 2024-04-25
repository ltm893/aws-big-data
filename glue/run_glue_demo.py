from datetime import date
import time
import random
import argparse
from argparse import RawTextHelpFormatter
import os
import boto3
import logging
import sys
import pandas as pd
import string
import io
from pprint import pprint
from cf_wrapper import CloudFormationWrapper
from create_test_data import make_person_json, make_zip_csv, upload_to_bucket, empty_bucket, check_create_testdata_dir
from glue_wrapper import GlueWrapper


today = date.today()
DATE_STRING = today.strftime("%Y%m%d")
DEMO_TYPE = 'gluedemo'

logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
logger = logging.getLogger(__name__)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
logger.addHandler(consoleHandler)

logger.setLevel(logging.INFO)

class GlueDemo(): 
    

    def __init__(self, cf_client,s3_resource,glue_client,iam_resource):
        self.glue_client = glue_client
        self.s3_resource = s3_resource
        self.iam_resource = iam_resource
        self.cf_client = cf_client
        self.glue_wrapper = GlueWrapper(glue_client)    


    def make_config(self,base_name,type):
        self.name = f'{base_name}-{DEMO_TYPE}'
        self.stack_name = f'{self.name}-stack'
        self.stack_template = os.path.join(os.getcwd(), "cloudformation","create_glue_role_bucket.yaml")
        self.role_name = 'DemoGlueServiceRole1'
        self.glue_db_name = f'{base_name}-{DEMO_TYPE}-database'
        self.glue_crawler_name = f'{base_name}-{DEMO_TYPE}-crawler'
        if(type == 'new'):
            self.bucket_name  = f'{DEMO_TYPE}-{DATE_STRING}-{base_name}-{GlueDemo.__make_rs(6)}'
        if(type == 'existing'):
            self.bucket_name  = GlueDemo.__get_bucket_name_from_stack_name(self,self.stack_name)
        self.bucket_uri = f's3://{self.bucket_name}'
        self.bucket_output = f'{self.bucket_name}/output'
        self.bucket_output_dated = f'{self.bucket_output}/{DATE_STRING}'
        self.glue_job_script_local = os.path.join('job_scripts','person_zip_join_to_parquet.py')
        self.glue_job_script_s3_key  = 'job_scripts/person_zip_join_to_parquet.py'
        self.glue_job_script_s3_full = f'{self.bucket_name}/{self.glue_job_script_s3_key}'
        self.glue_job_name       = 'person_zip_join_to_parquet' 
        self.test_data_dir = os.path.join(os.getcwd(), "testdata")
        self.person_test_file = f'{self.test_data_dir}/people.json.gz' 
        self.person_test_s3 = 'json/people.json.gz'
        self.zip_test_file = f'{self.test_data_dir}/zip_group.csv.gz'
        self.zip_test_s3   = 'csv/test_zip_group.gz'
        self.glue_prefix_exclusions = ['job_scripts/**', 'output/**'] 

    def print_select_props(self):    
        print("Attributes of your Demo Object")
        [print(k, v,  sep=': ') for k,v in vars(self).items() if k not in ['glue_client', 's3_resource', 'cf_wrapper', 'glue_wrapper'  ] ]


   
    def __make_rs(num):
        chars = string.ascii_lowercase + string.digits
        return ''.join(random.choice(chars) for _ in range(num) )


    def get_user_base_name_for_config(self,type,destroy=None) :  
        if type == 'new':
            input_message = 'Enter a friendly alpha numeric name less than 20 characters: '
        if type == 'existing':
             input_message = 'Enter friendly name: used in deploy phase: ' 
        while True :
            
            base_name = input(input_message)
            if base_name.isalnum() and len(base_name) <= 20 :
                self.make_config(base_name,type)
                
                if destroy is None: self.print_select_props() 
                if(type == 'new'):
                    create_stack = input('Start Glue Demo with above details?  y/n: ')
                    if (create_stack == 'y'):
                        self.deploy_bucket_role_stack()
                    else :
                        continue

                self.iam_role_arn = self.iam_resource.Role(self.role_name).arn
                return
            else :
                print(base_name, 'base name not valid')


    @staticmethod
    def wait(seconds, tick=12):
        """
        Waits for a specified number of seconds, while also displaying an animated
        spinner.

        :param seconds: The number of seconds to wait.
        :param tick: The number of frames per second used to animate the spinner.
        """
        progress = "|/-\\"
        waited = 0
        while waited < seconds:
            for frame in range(tick):
                sys.stdout.write(f"\r{progress[frame % len(progress)]}")
                sys.stdout.flush()
                time.sleep(1 / tick)
            waited += 1

    def __get_bucket_name_from_stack_name(self,stack_name):
        cf_wrapper =CloudFormationWrapper(self.cf_client) 
        stack_response = cf_wrapper.list_stack_resources(StackName=stack_name)
        if 'StackResourceSummaries' in stack_response:
            for res in stack_response['StackResourceSummaries']:
                if res['LogicalResourceId'] == 'DemoGlueS3Bucket' :
                    return res['PhysicalResourceId'] 
        return stack_response

    def deploy_bucket_role_stack(self):
        
        s3_resource = self.s3_resource

        cf_wrapper =CloudFormationWrapper(self.cf_client) 
        cf_wrapper.create_stack(Capabilities=['CAPABILITY_NAMED_IAM'], StackName=self.stack_name, 
            TemplateBody=self.stack_template, Parameters=[{ 'ParameterKey': 'BucketName', 'ParameterValue': self.bucket_name},
                                                          {'ParameterKey': 'GlueRole', 'ParameterValue': self.role_name} ] )
        
        check_create_testdata_dir()

        print("Making person objects")
        make_person_json(self.person_test_file)

        print("Uploading %s to s3://%s" % (self.person_test_file , self.bucket_name + '/json'))
        bucket = s3_resource.Bucket(self.bucket_name)
        upload_to_bucket(bucket,self.person_test_file, self.person_test_s3,s3_resource)

        print("Making zip csv file")
        make_zip_csv(self.zip_test_file)

        print("Uploading %s to s3://%s" % (self.zip_test_file, self.bucket_name + '/csv') )
        upload_to_bucket(bucket,self.zip_test_file ,self.zip_test_s3, s3_resource)

        print("Uploading %s to %s" % ( self.glue_job_script_local,self.bucket_name + '/scripts' ) )
        upload_to_bucket(bucket,self.glue_job_script_local,self.glue_job_script_s3_key,s3_resource)  

    def destory_demo(self):
        s3_resource = self.s3_resource
        wrapper = self.glue_wrapper
        stack_name = self.stack_name
        cf_wrapper =CloudFormationWrapper(self.cf_client) 
        stack_response = cf_wrapper.list_stack_resources(StackName=stack_name)
        if 'StackResourceSummaries' in stack_response:
            print()
            print("All Resource below will be deleted")
            print()
            for obj in stack_response['StackResourceSummaries']:
                print('Cloud Formation Resource', obj['ResourceType'], obj['PhysicalResourceId'],  sep=' --> ')
            db_check = wrapper.get_database(self.glue_db_name)
            if db_check:
                print('glue database',db_check['Name'], sep=': ') 

            crawler_check =  wrapper.get_crawler(self.glue_crawler_name)
            if crawler_check:
                print('glue crawler', crawler_check['Name'], sep=': ')
              
            job_check = wrapper.get_job(self.glue_job_name)
            if job_check:
                print('glue job', job_check['Name'], sep=': ')
            
            user_del = input("Delete Stack and Glue Resources y/n: ")
            if user_del == 'y':
                empty_bucket(GlueDemo.__get_bucket_name_from_stack_name(self,stack_name),s3_resource)
                print(f'Preparing to delete Stack{self.stack_name}')        
                cf_wrapper.delete_stack(self.stack_name)
                if wrapper.get_database(self.glue_db_name):
                    wrapper.delete_database(self.glue_db_name)
                    print(f'Deleted {self.glue_db_name}')
                if wrapper.get_crawler(self.glue_crawler_name):
                    wrapper.delete_crawler(self.glue_crawler_name)
                    print(f'Deleted {self.glue_crawler_name}')
                if wrapper.get_job(self.glue_job_name) :
                    wrapper.delete_job(self.glue_job_name)
                    print(f'Deleted {self.glue_job_name}')
                return
            else:
                print('Pick another name')

        else :
            print(f'{self.stack_name} not found')
            return
    
    def get_create_crawler(self):
        wrapper = self.glue_wrapper
        crawler_name =  self.glue_crawler_name
        glue_service_role_arn = self.iam_role_arn
        db_name =  self.glue_db_name
        db_prefix = self.name + '-'
        data_source =  self.bucket_uri
        exlusions =  self.glue_prefix_exclusions

        print(f"Checking for crawler {crawler_name}.")
        crawler = wrapper.get_crawler(crawler_name)
        print(f"Found existing crawler {crawler_name}.")
        if crawler is None:
            print(f"Creating crawler {crawler_name}.")
            wrapper.create_crawler(
                crawler_name,
                glue_service_role_arn,
                db_name,
                db_prefix,
                data_source,
                exlusions
            )
            print(f"Created crawler {crawler_name}.")
            crawler = wrapper.get_crawler(crawler_name)

        pprint(crawler) 
       
    def start_crawler(self):
        wrapper = self.glue_wrapper
        wrapper.start_crawler(self.glue_crawler_name)
        print("Let's wait for the crawler to run. This typically takes a few minutes.")
        crawler_state = None
        while crawler_state != "READY":
            self.wait(10)
            crawler = wrapper.get_crawler(self.glue_crawler_name)
            crawler_state = crawler["State"]
            print(f"Crawler is {crawler['State']}.")

        database = wrapper.get_database(self.glue_db_name)
        print(f"The crawler database {self.glue_db_name}:")
        pprint(database)
        print(f"Glue Crawler has updated these tables:")
        tables = wrapper.get_tables(self.glue_db_name)
        for table in tables:
            print(f"{table['Name']}")
            if table['Name'].endswith('json'):
                self.glue_json_table = table['Name']
            if table['Name'].endswith('csv'):
                self.glue_csv_table = table['Name']

    def create_run_job(self):
        wrapper = self.glue_wrapper
        job = wrapper.get_job(self.glue_job_name)
        if job is None:
            wrapper.create_job(
                self.glue_job_name,
                f'{self.name}-example',
                self.iam_role_arn,
                f"s3://{self.glue_job_script_s3_full}",
            )
        job_run_status = None
        job_run_id = wrapper.start_job_run(
                self.glue_job_name, self.glue_db_name, self.glue_json_table, self.glue_csv_table , self.bucket_output_dated
            )
        print(f"Job {self.glue_job_name} started. Let's wait for it to run.")
        while job_run_status not in ["SUCCEEDED", "STOPPED", "FAILED", "TIMEOUT"]:
            self.wait(10)
            job_run = wrapper.get_job_run(self.glue_job_name, job_run_id)
            job_run_status = job_run["JobRunState"]
            print(f"Job {self.glue_job_name}/{job_run_id} is {job_run_status}.")
    
    def s3_data(self):
        try:
            bucket_keys =  [ obj.key for obj in self.s3_resource.Bucket(self.bucket_name).objects.filter(Prefix="output") ]
            data = io.BytesIO()
            self.s3_resource.Bucket(self.bucket_name).download_fileobj(bucket_keys[0],data)
            df = pd.read_parquet(data)
            print()
            print(f'Displaying sample from s3://{self.bucket_name}/{bucket_keys[0]}')
            print()
            print(df.head())
        except Exception as err:
            logger.error  (f'Couldn\'t display data {err}')
            raise

def parse_args(args):
    """
    Parse command line arguments.

    :param args: The command line arguments.
    :return: The parsed arguments.
    """
    parser = argparse.ArgumentParser(
            description="Deploys S3 bucket, IAM role, creates Glue Crawler with test data "
            "Crawler joins json data with csv and stores in a parquet format in output prefix",  formatter_class=RawTextHelpFormatter
        )
    parser.add_argument(
        "action",
        choices=["deploy", "destroy", "crawler"],
        help="deploy, creates componets named based on user supplied friendly name  \n"
        "destroy, removes existing componets based on user supplied friendly name \n"
        "crawler, runs crawler on existing componets"
    )
    return parser.parse_args(args)


def main():
    args = parse_args(sys.argv[1:])
 
    gluedemo = GlueDemo(boto3.client("cloudformation"),
                        boto3.resource("s3"),
                        boto3.client("glue"),
                        boto3.resource("iam")
    )

    if args.action == "deploy":
        gluedemo.get_user_base_name_for_config('new')
            
    elif args.action == "destroy":
        gluedemo.get_user_base_name_for_config('existing','d')
        gluedemo.destory_demo()

    elif args.action == "crawler":
        gluedemo.get_user_base_name_for_config('existing')
        
    if args.action == "deploy" or args.action == "crawler" :
        gluedemo.get_create_crawler()
        gluedemo.start_crawler()
        gluedemo.create_run_job()
        gluedemo.s3_data()

if __name__ == '__main__':
    main()
    

   
