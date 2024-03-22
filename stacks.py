
import boto3
from botocore.exceptions import ClientError
import logging
import json
import os

import argparse
from make_people import create_objects


















if __name__ == '__main__' :
   
    script_name = os.path.basename(__file__)
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh = logging.FileHandler(f'logs/{script_name}.log')
    fh.setFormatter(formatter)
    logger.addHandler(fh)



    parser = argparse.ArgumentParser('Create or Delete Cloud Formation Stack')
    parser.add_argument('stack_operation',  choices=['create', 'delete'],   help='increase output verbosity')
    parser.add_argument('aws_profile',   help='AWS authenticated profile')
    parser.add_argument('--bucket_name', help='Unique bucket. Will remove and recreate bucket')
    args = parser.parse_args()

    print(args)

    session = boto3.session.Session(profile_name=args.profile_name)
    cf_client = session.client('cloudformation')
    people_filenname = 'output/people.json.gz'

    if args.stack_operation == "create":
        message = 'creating stack'
        logger.info(message)
        make_person_json(people_filenname)
    

    if args.stack_operation == "delete":
        message = 'deleting stack' 
        logger.info(message)


