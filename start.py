from datetime import date
import argparse
import os
import boto3
import logging
import sys
import pandas as pd

for dir in ["cloudformation","make_people","glue"] : 
    sys.path.append(os.path.join(os.getcwd(), dir))

from cloudformation.stacks import *
from make_people.create_objects import *
from glue.crawler import *
from glue.glue_wrapper import GlueWrapper

today = date.today()
DATE_STRING = today.strftime("%Y%m%d")
DEMO_TYPE = 'gluedemo'

logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
logger = logging.getLogger(__name__)

fileHandler = logging.FileHandler("{0}/{1}.log".format('logs','__name__.log' ))
fileHandler.setFormatter(logFormatter)
logger.addHandler(fileHandler)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
logger.addHandler(consoleHandler)

logger.setLevel(logging.INFO)

class GlueDemo(): 
    

    def __init__(self, cf_client,s3_resource,glue_client):
        self.glue_client = glue_client
        self.s3_resource = s3_resource
        self.cf_client = cf_client
        self.glue_wrapper = GlueWrapper(self.glue_client)    


    def make_config(self,base_name,type):
        self.name = f'{base_name}-{DEMO_TYPE}'
        self.stack_name = f'{self.name}-stack'
        self.stack_template = os.path.join(os.getcwd(), "cloudformation","templates","create_glue_role_bucket.yaml")
        self.role_name = 'DemoGlueServiceRole1'
        self.glue_db_name = f'{base_name}-{DEMO_TYPE}-database'
        self.glue_crawler_name = f'{base_name}-{DEMO_TYPE}-crawler'
        if(type == 'new'):
            self.bucket_name  = f'{DEMO_TYPE}-{DATE_STRING}-{base_name}-{self.make_rs(6)}'
        if(type == 'existing'):
            self.bucket_name  = GlueDemo.__get_bucket_name_from_stack_name(self,self.stack_name)
        self.bucket_uri = f's3://{self.bucket_name}'
        self.bucket_output = f'{self.bucket_name}/output'
        self.bucket_output_dated = f'{self.bucket_output}/{DATE_STRING}'
        self.glue_job_script_local = os.path.join('glue','job_scripts','person_zip_join_to_parquet.py')
        self.glue_job_script_s3_key  = 'job_scripts/person_zip_join_to_parquet.py'
        self.glue_job_script_s3_full = f'{self.bucket_name}/{self.glue_job_script_s3_key}'
        self.glue_job_name       = 'person_zip_join_to_parquet' 
        self.test_data_dir = os.path.join(os.getcwd(), "testdata")
        self.person_test_file = f'{self.test_data_dir}/people.json.gz' 
        self.person_test_s3 = 'json/people.json.gz'
        self.zip_test_file = f'{self.test_data_dir}/zip_group.csv.gz'
        self.zip_test_s3   = 'csv/test_zip_group.gz'

    def print_select_props(self):
            props = f''' 
            Config Properties
            Cloudformation Stack Name: {self.stack_name }
            S3 Bucket: {self.bucket_name}
            S3 Files : [ "{self.person_test_s3}", "{self.zip_test_s3}", "{self.glue_job_script_s3_full}" ]
            GLue DB Name: {self.glue_db_name}
            Crawler Name: {self.glue_crawler_name}

            ''' 
            print(props)

    @staticmethod
    def make_rs(num):
        chars = string.ascii_lowercase + string.digits
        return ''.join(random.choice(chars) for _ in range(num) )


    def get_user_base_name_for_config(self,type) :  
        if type == 'new':
            input_message = 'Please enter an alpha numeric friendly name less than 20 characters for base name: '
        if type == 'existing':
             input_message = 'Please enter your existing friendly name: ' 
        while True :
            base_name = input(input_message)
            if base_name.isalnum() and len(base_name) <= 20 :
                self.make_config(base_name,type)
                
                self.print_select_props()
                if(type == 'new'):
                    create_stack = input('Creating above stack  y/n: ')
                    if (create_stack == 'y'):
                        GlueDemo.__deploy_bucket_role_stack(self)
                iam_role = boto3.resource("iam").Role(self.role_name)

                self.iam_role_arn = iam_role.arn
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
        stack_response = list_stack_resources(self.cf_client,StackName=stack_name)
        if 'StackResourceSummaries' in stack_response:
            for res in stack_response['StackResourceSummaries']:
                if res['LogicalResourceId'] == 'DemoGlueS3Bucket' :
                    return res['PhysicalResourceId'] 

    def __deploy_bucket_role_stack(self):
        cf_client = self.cf_client
        s3_resource = self.s3_resource

        create_stack(cf_client, Capabilities=['CAPABILITY_NAMED_IAM'], StackName=self.stack_name, 
            TemplateBody=self.stack_template, Parameters=[{ 'ParameterKey': 'BucketName', 'ParameterValue': self.bucket_name},
                                                         { 'ParameterKey': ' GlueRole', 'ParameterValue': self.role_name} ] )
        
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
        cf_client = self.cf_client
        while True :
            stack_name = input("Stack name: ")
            stack_response = list_stack_resources(cf_client,StackName=stack_name)
            if 'StackResourceSummaries' in stack_response:
                print()
                print("All Resource below will be deleted")
                print()
                pprint(stack_response)
                print()
                user_del = input("Delete Stack y/n: ")
                if user_del == 'y':
                    #empyt bucket for stack delete
                    for res in stack_response['StackResourceSummaries']:
                        if res['LogicalResourceId'] == 'DemoGlueS3Bucket' :
                            bucket_name = res['PhysicalResourceId'] 
                            empty_bucket(bucket_name,s3_resource)
                    delete_stack(cf_client,stack_name)
                    return
                else:
                    print('Pick another stack')

            else :
                print("Unexpected response")
                return
    
    def get_create_crawler(self):
        wrapper = self.glue_wrapper
        crawler_name =  self.glue_crawler_name
        glue_service_role_arn = self.iam_role_arn
        db_name =  self.glue_db_name
        db_prefix = self.name + '-'
        data_source =  self.bucket_uri

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
            )
            print(f"Created crawler {crawler_name}.")
            crawler = wrapper.get_crawler(crawler_name)

        pprint(crawler) 
       
    def start_crawler(self):
        # wrapper = GlueWrapper(self.glue_client)
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

    def run_job(self):
        wrapper = self.glue_wrapper
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
        bucket_keys =  [ obj.key for obj in self.s3_resource.Bucket(self.bucket_name).objects.filter(Prefix="output") ]
        data = io.BytesIO()
        self.s3_resource.Bucket(self.bucket_name).download_fileobj(bucket_keys[0],data)
        df = pd.read_parquet(data)
        print()
        print(f'Displaying sample from s3://{self.bucket_name}/{bucket_keys[0]}')
        print()
        print(df.head())

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
            description="Deploys and destroys s3 Bucket, IAM Role and Test Data"
        )
    parser.add_argument(
        "action",
        choices=["deploy", "destroy", "crawler"],
        help="Indicates the action that the script performs.",
    )
    
    args = parser.parse_args()
 
    gluedemo = GlueDemo(boto3.client("cloudformation"),
                        boto3.resource("s3"),
                        boto3.client("glue"))

    if args.action == "deploy":
        gluedemo.get_user_base_name_for_config('new')
            
    elif args.action == "destroy":
        gluedemo.destory_demo()

    elif args.action == "crawler":
        gluedemo.get_user_base_name_for_config('existing')
        
    if args.action == "deploy" or args.action == "crawler" :
        gluedemo.get_create_crawler()
        gluedemo.start_crawler()
        gluedemo.run_job()
        gluedemo.s3_data()

   
