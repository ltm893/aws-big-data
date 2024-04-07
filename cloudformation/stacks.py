
import boto3
from botocore.exceptions import ClientError
import logging
import json
import os

import argparse

stack_arn = ''

''' 
with open('batch-job-cft.yml', 'r') as cf_file:
    cft_template = cf_file.read()
    cf_client.create_stack(StackName='Batch Job', TemplateBody=cft_template)
'''

def create_stack (cf_client, **kwargs ):
    print(kwargs['TemplateBody'])
    with open( kwargs['TemplateBody'],'r') as cf_file:
        kwargs['TemplateBody'] = cf_file.read()
   #stack_kwargs = {k: v for k, v in kwargs.items() if v is not None}
   
         #aws cloudformation create-stack 
   
    response = cf_client.create_stack(**kwargs)       

    stack_arn = response['StackId']

    
    waiter = cf_client.get_waiter('stack_create_complete')
    print('Starting Stack Wait for:', kwargs['StackName'])
    print('StackId:', stack_arn)
    waiter.wait(
        StackName=kwargs['StackName'],
        WaiterConfig={
            'Delay': 30,
            'MaxAttempts': 10
        }
    )
    print('Stack Complete for:', stack_arn)

def delete_stack(cf_client,stack_name):
    response = cf_client.delete_stack(StackName=stack_name)
    print('Starting Stack Wait Delete for:', stack_name)
    waiter = cf_client.get_waiter('stack_delete_complete')

    waiter.wait(
        StackName=stack_name,
        WaiterConfig={
            'Delay': 30,
            'MaxAttempts': 10
        }
    )
    print('Stack Delete Complete for:', stack_name)



if __name__ == '__main__' :
    create_stack_2('client',
        Capabilities=['CAPABILITY_NAMED_IAM'], StackName='stack_name', TemplateBody='cf_template', Parameters=[{ 'ParameterKey': 'bucketName', 'ParameterValue': 'joejoen'}] )

    ''' 
   
    script_name = os.path.basename(__file__)
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    # formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
   #  fh = logging.FileHandler(f'logs/{script_name}.log')
    # fh.setFormatter(formatter)
    # logger.addHandler(fh)



    parser = argparse.ArgumentParser('Create or Delete Cloud Formation Stack')
    parser.add_argument('stack_operation',  choices=['create', 'delete'],   help='increase output verbosity')
    parser.add_argument('aws_profile',   help='AWS authenticated profile')
    # parser.add_argument('--bucket_name', help='Unique bucket. Will remove and recreate bucket')
    args = parser.parse_args()
    #print(args)

    session = boto3.session.Session(profile_name=args.aws_profile)
    cf_client = session.client('cloudformation')
    stack_name = 'glue-stack-1'
    cf_template = '../cf_templates/create_glue_role_bucket.yaml'

    if args.stack_operation == "create":
        print(f'Creating {stack_name}')
        create_stack(cf_client,stack_name,cf_template)      
    

    if args.stack_operation == "delete":
        print(f'Deleting  {stack_name}') 
        delete_stack(cf_client,stack_name)   

'''
