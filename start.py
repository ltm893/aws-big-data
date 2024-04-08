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
           


    def make_config(self,base_name):
        self.name = f'{base_name}-{DEMO_TYPE}'
        self.stack_name = f'{self.name}-stack'
        self.stack_template = os.path.join(os.getcwd(), "cloudformation","templates","create_glue_role_bucket.yaml")
        self.role_name = 'DemoGlueServiceRole1'
        self.glue_db_name = f'{base_name}-{DEMO_TYPE}-database'
        self.glue_crawler_name = f'{base_name}-{DEMO_TYPE}-crawler'
        self.bucket_name  = f'{DEMO_TYPE}-{DATE_STRING}-{base_name}-{GlueDemo.__make_rs(6)}'
        self.bucket_uri = f's3://{self.bucket_name}'
        self.bucket_output_uri = f'{self.bucket_uri}/output'
        self.bucket_output_dated_uri = f'{self.bucket_output_uri}/{DATE_STRING}'
        self.glue_job_script_local = os.path.join('glue','job_scripts','person_zip_join_to_parquet.py')
        self.glue_job_script_s3  = 'job_scripts/person_zip_join_to_parquet.py'
        self.test_data_dir = os.path.join(os.getcwd(), "testdata")
        self.person_test_file = f'{self.test_data_dir}/people.json.gz' 
        self.person_test_s3 = 'json/people.json.gz'
        self.zip_test_file = f'{self.test_data_dir}/zip_group.csv.gz'
        self.zip_test_s3   = 'csv/test_zip_group.gz'

    
    def print_select_props(self):
            props = f''' 
            Cloudformation Stack Name: {self.stack_name }
            S3 Bucket: {self.bucket_name}
            S3 Files : [ "{self.person_test_s3}", "{self.zip_test_s3}", "{self.glue_job_script_s3}" ]
            ''' 
            print(props)


    def __make_rs(num):
        chars = string.ascii_lowercase + string.digits
        return ''.join(random.choice(chars) for _ in range(num) )


    def make_demo_from_user(self) :  
        while True :
            base_name = input("Please enter an alpha numeric friendly name less than 20 characters for base name: ")
            if base_name.isalnum() and len(base_name) <= 20 :
                self.make_config(base_name)
                
                self.print_select_props()

                create_stack = input('Creating above stack  y/n: ')
                if (create_stack == 'y'):
                    GlueDemo.__deploy_bucket_role_stack(self)
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

    def __deploy_bucket_role_stack(self):
        cf_client = self.cf_client
        s3_resource = self.s3_resource

        create_stack(cf_client, Capabilities=['CAPABILITY_NAMED_IAM'], StackName=self.stack_name, 
            TemplateBody=self.stack_template, Parameters=[{ 'ParameterKey': 'BucketName', 'ParameterValue': self.bucket_name}])
                                                        #  { 'ParameterKey': 'RoleArn', 'ParameterValue': self.role_arn} ] )
        
        iam_role = boto3.resource("iam").Role(self.role_name)
        print(iam_role.arn)
        self.iam_role_arn = iam_role.arn
        
        
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
        upload_to_bucket(bucket,self.glue_job_script_local,self.glue_job_script_s3,s3_resource)  

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
        wrapper = GlueWrapper(self.glue_client)
        crawler_name =  self.glue_crawler_name
        glue_service_role_arn = self.iam_role_arn
        db_name =  self.glue_db_name
        db_prefix = self.name
        data_source =  self.bucket_uri

        print(f"Checking for crawler {crawler_name}.")
        crawler = wrapper.get_crawler(crawler_name)
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
        wrapper = GlueWrapper(self.glue_client)
        wrapper.start_crawler(self.glue_crawler_name)
        print("Let's wait for the crawler to run. This typically takes a few minutes.")
        crawler_state = None
        while crawler_state != "READY":
            self.wait(10)
            crawler = wrapper.get_crawler(self.glue_crawler_name)
            crawler_state = crawler["State"]
            print(f"Crawler is {crawler['State']}.")
        

        tables = wrapper.get_tables(self.glue_db_name)
        pprint(tables)

        '''  
        wrapper.create_partition(DatabaseName='string',
                                    TableName='string',
                                    PartitionInput={ 'Values': ['string' ] })
        '''
      
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
        gluedemo.make_demo_from_user()
        gluedemo.get_create_crawler()
        gluedemo.start_crawler()
            
    elif args.action == "destroy":
        gluedemo.destory_demo()
        

    elif args.action == "crawler":
        pass

   
