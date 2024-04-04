#import create_objects
from create_objects import make_random_string, make_random_digit_string, upload_to_bucket, create_bucket, delete_bucket_by_name
import pytest
import boto3
from boto3.s3.transfer import S3UploadFailedError
from botocore.exceptions import ClientError
from tempfile import NamedTemporaryFile
from uszips import all_zips



@pytest.fixture
def bucket_name():
    return "my-test-bucket5"

@pytest.fixture
def create_bucket_for_testing(s3_resource, bucket_name):
    bucket = create_bucket(bucket_name,s3_resource)
    return bucket
    
@pytest.mark.aws_moto
def test_create_bucket(s3_resource, bucket_name):
    bucket = create_bucket(bucket_name,s3_resource)
    assert bucket.name == bucket_name

@pytest.mark.aws_moto
def test_delete_bucket(s3_resource, create_bucket_for_testing):
    bucket_name = create_bucket_for_testing.name
    delete_bucket_by_name(bucket_name,s3_resource)
    response =  s3_resource.meta.client.list_buckets()
    list = [b['Name'] for b in response['Buckets'] if b['Name'] == bucket_name ] 
    assert len(list) == 0   
   
@pytest.mark.aws_moto
def test_upload_to_bucket(s3_resource,create_bucket_for_testing):
    file_text = "test"
    with NamedTemporaryFile(delete=True, suffix=".txt") as tmp:
        with open(tmp.name, "w", encoding="UTF-8") as f:
            f.write(file_text)
            upload_to_bucket(create_bucket_for_testing,tmp.name,"keyname",s3_resource)

@pytest.mark.local_unit
def test_make_random_string():
    assert make_random_string(5) != make_random_string(5), 'ere equal'
    assert len(make_random_string(7)) <= 7,"length test"
    assert make_random_string(7)[0].isupper() == True, "first character not uppper"
    assert any(char.isdigit() for char in make_random_string(7)) == False, 'has digit'

@pytest.mark.local_unit
def test_make_random_digit_string():
    assert make_random_digit_string(5) != make_random_digit_string(5),'ere equal'
    assert len(make_random_digit_string(7)) <= 7,"length test" 
    assert make_random_digit_string(7)[0] != '0' , "first character zerpr"
    assert any(char.isalpha() for char in make_random_digit_string(9) ) == False, 'has letter' 

@pytest.mark.local_unit
def test_create_zips():
    assert len(all_zips) > 25000 ,'us_zips not long enough'