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



def test_get_create_crawler(make_stubber, stub_runner):
    cf_client = boto3.client("cloudformation")
    cf_stubber = make_stubber(cf_client)
    s3_resource = boto3.resource("s3")
    s3_stubber = make_stubber(s3_resource)
    glue_client = boto3.client("glue")
    glue_stubber = make_stubber(glue_client)

    gluedemo = GlueDemo(boto3.client("cloudformation"),
                    boto3.resource("s3"),
                    boto3.client("glue"))

    #inputs = [ "card233", "y" ]
    # monkeypatch.setattr("builtins.input", lambda x: inputs.pop(0))
   #  monkeypatch.setattr(time, "sleep", lambda x: None)

    stackname = 'stackname'

    cf_stubber.stub_create_stack(Capabilities=['CAPABILITY_NAMED_IAM'], StackName=self.stack_name, 
            TemplateBody=self.stack_template, Parameters=[{ 'ParameterKey': 'BucketName', 'ParameterValue': self.bucket_name},
                                                          {'ParameterKey': 'GlueRole', 'ParameterValue': self.role_name} ])
    

    gluedemo.stub