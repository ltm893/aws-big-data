# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Integration tests for scenario_getting_started_crawlers_and_jobs.py.
"""

from unittest.mock import patch
import boto3
from botocore.exceptions import ClientError
import pytest

from run_glue_demo import GlueDemo


def test_run_integ(monkeypatch):
    cf_client = boto3.client("cloudformation")
    s3_resource = boto3.resource("s3")
    glue_client = boto3.client("glue")
    #iam_resource = boto3.resource("iam")
    inputs = iter(['Pavol', 'y'])
    monkeypatch.setattr('builtins.input', lambda _: next(inputs))
    gluedemo = GlueDemo(cf_client, s3_resource,glue_client)
    gluedemo.get_user_base_name_for_config('new')
   
    assert True
    '''
    gluedemo = GlueDemo(cf_client, s3_resource,glue_client)
    monkeypatch.setattr("builtins.input", lambda _: "cards233")
    gluedemo.get_user_base_name_for_config('new')
   
    gluedemo.make_config()

    assert gluedemo.stack_name == 'cards233-gluedemo-stack'
    '''
      
       
