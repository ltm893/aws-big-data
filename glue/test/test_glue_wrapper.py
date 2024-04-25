from glue_wrapper import GlueWrapper
import pytest
import time
from uuid import uuid4
import boto3
from botocore.client import ClientError
from botocore.exceptions import ParamValidationError


@pytest.fixture
def crawler_dict():
    cd = {}
    cd['name'] = 'testing-crawler'
    cd['role'] = 'testing-crawler-role'
    cd['db_name'] = 'testing-crawler-database'
    cd['db_prefix'] = 'testing-crawler-prefix'
    cd['targets']   = 'testing-crawler-targets'
    cd['exclusions'] = ['job_scripts/**', 'output/**']
    return cd

def create_test_crawler(glue_client,crawler_dict):
    exclusions = []
    exclusions.append('job_scripts/**')
    exclusions.append('output/**')
    glue_client.create_crawler(
        Name=crawler_dict['name'],
        Role=crawler_dict['role'],
        Targets={"S3Targets": [{"Path": "s3://tests3target"}]}
    )
    
@pytest.mark.aws_moto
def test_get_crawler(glue_client,crawler_dict):
    gw = GlueWrapper(glue_client)
    nocrawler = gw.get_crawler(crawler_dict['name'])
    assert nocrawler is None
    create_test_crawler(glue_client,crawler_dict)
    crawler = gw.get_crawler(crawler_dict['name'])
    assert crawler['Name'] == crawler_dict['name']

@pytest.mark.aws_moto
def test_create_crawler(glue_client,crawler_dict):
    gw = GlueWrapper(glue_client)
    gw.create_crawler(
            crawler_dict['name'],
            crawler_dict['role'],
            crawler_dict['db_name'],
            crawler_dict['db_prefix'],
            crawler_dict['targets'],
            crawler_dict['exclusions']
        )
    crawler = gw.get_crawler(crawler_dict['name'])
    gw.start_crawler(crawler_dict['name'])
    assert crawler['Name'] == crawler_dict['name']
    assert crawler['State'] == 'READY'
    # database = gw.get_database(crawler_dict['db_name'])
   # tables = gw.get_tables(crawler_dict['db_name'])
   # assert database['Database']['Name'] == crawler_dict['db_name']

@pytest.mark.aws_moto
def test_create_job(glue_client,crawler_dict):
    gw = GlueWrapper(glue_client)
    
    gw.create_job(
         'name', 
         'description', 
         crawler_dict['role'], 
         'script_location'

    )
    
    response = gw.start_job_run (
        'name',
        crawler_dict['db_name'],
        'glue_json_table', 
        'glue_csv_table' , 
        'bucket_output_dated',
    )
    assert response == '01'

   

