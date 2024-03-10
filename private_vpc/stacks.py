import sys
import boto3
import argparse
import logging
import json
import os

script_name = os.path.basename(__file__)
session = boto3.session.Session(profile_name='todd')
cf_client = session.client('cloudformation')

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh = logging.FileHandler(f'logs/{script_name}.log')
fh.setFormatter(formatter)
logger.addHandler(fh)

stack_arn = ''
def create_stack():
    #aws cloudformation create-stack 
    stack_name = 'spark-cluster-1-stack'
    cf_template = open('CloudFormationPubPrivate.yaml').read()
    response = cf_client.create_stack(
                    StackName=stack_name, 
                    TemplateBody=cf_template,
                    Parameters=[ 
                                {'ParameterKey': 'EnvironmentName','ParameterValue': 'dev'},
                                {'ParameterKey': 'VpcCIDR','ParameterValue': '10.14.0.0/16'},
                                {'ParameterKey': 'PublicSubnet1CIDR', 'ParameterValue': '10.14.10.0/24'},
                                {'ParameterKey': 'PrivateSubnet1CIDR','ParameterValue': '10.14.20.0/24'}
                    ],
                    Tags=[
                        {'Key': 'EnvironmentName','Value': 'dev'}
                    ]
                )

    stack_arn = response['StackId']

    
    waiter = cf_client.get_waiter('stack_create_complete')
    print('Starting Stack Wait for:', stack_name)
    print('StackId:', stack_arn)
    waiter.wait(
        StackName=stack_name,
        WaiterConfig={
            'Delay': 30,
            'MaxAttempts': 10
        }
    )
    print('Stack Complete for:', stack_arn)



def delete_stack(stack_name):
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



if __name__ == '__main__':
   
    parser = argparse.ArgumentParser("Create or Delete Cloud Formation Stack")
    subparser = parser.add_subparsers(dest = "command")

    create_stack_parser = subparser.add_parser("create", help="create stack")
   
    delete_stack_parser = subparser.add_parser("delete", help="delete stack")
    delete_stack_parser.add_argument("identifier", help="stack name or arn" )

    args = parser.parse_args()

    #print(args)

    if args.command == "create":
        message = 'creating stack'
        logger.info(message)
        print(message)
        create_stack()

    if args.command == "delete":
        print(args.identifier)
        stack = args.identifier
        message = 'deleting stack' + stack
        logger.info(message)
        print(message,stack)
        delete_stack(stack)

