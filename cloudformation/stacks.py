
import boto3
from botocore.exceptions import ClientError
import logging



logger = logging.getLogger(__name__)

stack_arn = ''

''' 
with open('batch-job-cft.yml', 'r') as cf_file:
    cft_template = cf_file.read()
    cf_client.create_stack(StackName='Batch Job', TemplateBody=cft_template)
'''

def create_stack (cf_client, **kwargs ):
    try: 
        print(kwargs['TemplateBody'])
        with open( kwargs['TemplateBody'],'r') as cf_file:
            kwargs['TemplateBody'] = cf_file.read()
        #stack_kwargs = {k: v for k, v in kwargs.items() if v is not None}
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
    except  ClientError as err:
        logger.error(
                "Couldn't create stack. Here's why: %s: %s",
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
        )
        raise

def delete_stack(cf_client,stack_name):
    try: 
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
    except  ClientError as err:
        logger.error(
                "Couldn't delete stack resouce. Here's why: %s: %s",
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
        )
        raise

def list_stack_resources(cf_client, **kwargs): 
    try: 
        response = cf_client.list_stack_resources(**kwargs)
    except  ClientError as err:
        logger.error(
                "Couldn't get stack resouce. Here's why: %s: %s",
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
        )
        raise

    return response



# if __name__ == '__main__' :
 