
import boto3
from botocore.exceptions import ClientError
import logging



logger = logging.getLogger(__name__)

class CloudFormationWrapper():

    def __init__(self, cf_client):
        self.cf_client = cf_client  

    def create_stack (self, **kwargs ):
        try: 
            print(kwargs['TemplateBody'])
            with open( kwargs['TemplateBody'],'r') as cf_file:
                kwargs['TemplateBody'] = cf_file.read()
            #stack_kwargs = {k: v for k, v in kwargs.items() if v is not None}
            response = self.cf_client.create_stack(**kwargs) 
            print(response)      
            stack_arn = response['StackId']

            
            waiter = self.cf_client.get_waiter('stack_create_complete')
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
        return

    def delete_stack(self,stack_name):
        try: 
            response = self.cf_client.delete_stack(StackName=stack_name)
            print('Starting Stack Wait Delete for:', stack_name)
            waiter = self.cf_client.get_waiter('stack_delete_complete')

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

    def list_stack_resources(self, **kwargs): 
        try: 
            response = self.cf_client.list_stack_resources(**kwargs)
        except  ClientError as err:
            logger.error(
                    "Couldn't get stack resouce. Here's why: %s: %s",
                    err.response["Error"]["Code"],
                    err.response["Error"]["Message"],
            )
            raise

        return response



if __name__ == '__main__' :
    cf_wrapper = CloudFormationWrapper(boto3.client("cloudformation"))
    response = cf_wrapper.create_stack(Capabilities=['CAPABILITY_NAMED_IAM'], StackName='myStack', 
            TemplateBody='cloudformation/create_glue_role_bucket.yaml', Parameters=[{ 'ParameterKey': 'BucketName', 'ParameterValue': 'ltm893-selfcheck-234'},
                                                          {'ParameterKey': 'GlueRole', 'ParameterValue':'DemoGlueServiceRole1'} ] )

    print(response)
        
 