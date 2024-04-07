import numpy as np
import gzip
import boto3
from botocore.exceptions import ClientError
import logging
import json
import random
import string
from .uszips import all_zips
import os
import jsonlines

#import timeit
logger = logging.getLogger(__name__)


def make_random_string(max_length):
    if max_length < 3 :
        raise Exception("max length parameter needs to be larger than 2")
    r_string = random.choice(list(string.ascii_uppercase))
    rr = random.randrange(3,max_length)
    for l in random.choices(list(string.ascii_lowercase),k=rr):
        r_string = r_string + l
    return  r_string


def make_random_digit_string(max_length):
    if max_length < 2 :
        raise Exception("max length parameter needs to be larger than 1")
    first_digit_choices = [ str(i) for i in range(1,10) ]
    other_digit_choices = [ str(i) for i in range(10) ]
    rr = random.randrange(3,max_length)
    r_string = random.choice(first_digit_choices)
    for l in random.choices(other_digit_choices,k=rr -1):
        r_string = r_string + l
    return r_string

def get_random_zip():
    return random.choice(all_zips)

    
def make_person():
    person = {}
    person['name'] = make_random_string(5) + ' ' + make_random_string(12)
    person['usage'] =  make_random_digit_string(6)
    person['zip']  = get_random_zip()
    return person
    
    

def make_zip_csv(zg_filename):
    zip_group =  [[z,random.randrange(1,11)]  for z in all_zips ]
    zip_group.insert(0,['zipcode','group'])
    np.savetxt(zg_filename, zip_group , delimiter =",",fmt ='% s')
   
def make_person_json(people_filenname):
    with gzip.open(people_filenname, 'wb') as fp:
        json_writer = jsonlines.Writer(fp)
        json_writer.write_all( [make_person()  for i in range(1,1000000) ])


    

def create_bucket(bucket_name,s3_resource):
    try:
        bucket = s3_resource.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={
                "LocationConstraint": s3_resource.meta.client.meta.region_name
            },
        )
        bucket.wait_until_exists()
        logger.info("Created bucket %s.", bucket_name)
    except ClientError:
        logger.exception("Couldn't create bucket %s.", bucket_name)
        raise

    return bucket



def upload_to_bucket(bucket, local_file_name, bucket_key, s3_resource):
    try:
        bucket.upload_file(local_file_name, bucket_key)
        logger.info(
            "Uploaded script %s to %s.", local_file_name, f"{bucket.name}/{bucket_key}"
        )
    except ClientError:
        logger.exception("Couldn't upload %s to %s.", local_file_name, bucket.name)
        raise

def delete_bucket_by_name(bucket_name,s3_resource):
    try:
        bucket = s3_resource.Bucket(bucket_name)
        if bucket.creation_date:
            print("%s exists" % bucket_name)
        else:
            print("%s does not exist" % bucket_name)
            return
        bucket.objects.delete()
        bucket.delete()
        logger.info("Emptied and removed bucket %s.", bucket.name)
        print(f"Emptied and removed bucket {bucket_name}")
    except ClientError:
        logger.exception(f'Couldn\'t remove bucket {bucket_name}')
        raise









