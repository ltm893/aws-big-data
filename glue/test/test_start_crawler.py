# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Unit tests for scenario_getting_started_crawlers_and_jobs.py functions.
"""

from datetime import datetime
import time
import pytest
import boto3
from boto3.exceptions import S3UploadFailedError
from botocore.exceptions import ClientError

from run_glue_demo import GlueDemo
from common import *
from stubber_factory import * 




@pytest.mark.parametrize(
    "error_code, stop_on_method",
    [
        (None, None),
        ("TestException", "stub_create_crawler")
       
    ],
)
def test_start_crawler(make_stubber, stub_runner, monkeypatch, error_code, stop_on_method):
    cf_client = boto3.client("cloudformation")
    cf_stubber = make_stubber(cf_client)
    glue_client = boto3.client("glue")
    glue_stubber = make_stubber(glue_client)
    s3_resource = boto3.resource("s3")
    s3_stubber = make_stubber(s3_resource.meta.client)
    
    crawler_name = "cards233-gluedemo-crawler"
    db_name = "cards233-gluedemo-database"
    db_prefix = "cards233-gluedemo-"
    s3_target = "test-s3-target"
    job_name = "test-job"
    role_name = "test-role"
    role_arn = "arn:aws:iam::123456789012:role/test-role"
    gluedemo = GlueDemo( cf_client, s3_resource , glue_client)
    gluedemo.make_config('cards233','new')
    gluedemo.iam_role_arn = role_arn

    tables = [
        {
            "Name": f"table-{index}",
            "DatabaseName": db_name,
            "CreateTime": datetime.now(),
        }
        for index in range(3)
    ]
    job_script = "test-script.py"
    run_id = "test-run-id"
    runs = [
        {
            "Id": f"{run_id}-{index}",
            "JobName": job_name,
            "CompletedOn": datetime.now(),
            "JobRunState": "SUCCEEDED",
        }
        for index in range(3)
    ]
    key = "run-1"
    run_data = b"test-data"

    inputs = ["y", "1", "y", "1", "1", "1", "y", "y", "y"]
    monkeypatch.setattr("builtins.input", lambda x: inputs.pop(0))
    monkeypatch.setattr(time, "sleep", lambda x: None)

    with stub_runner(error_code, stop_on_method) as runner:
        runner.add(
            glue_stubber.stub_create_crawler,
            crawler_name,
            role_arn,
            db_name,
            db_prefix,
            s3_target,
        )
        ''' 
        runner.add(
            glue_stubber.stub_start_crawler,
            crawler_name,
            error_code="EntityNotFoundException",
        )
        runner.add(
            glue_stubber.stub_get_crawler,
            crawler_name,
            error_code="EntityNotFoundException",
        )
        runner.add(glue_stubber.stub_get_crawler, crawler_name, "READY")
        runner.add(glue_stubber.stub_get_database, db_name)
        runner.add(glue_stubber.stub_get_tables, db_name, tables)
        
        '''
 
    if error_code is None:
        gluedemo.start_crawler()
    else:
        with pytest.raises(ClientError) as exc_info:
            gluedemo.start_crawler()
        assert exc_info.value.response["Error"]["Code"] == error_code
