# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Integration tests for scenario_getting_started_crawlers_and_jobs.py.
"""

from unittest.mock import patch
import boto3
from botocore.exceptions import ClientError
import pytest
from common import *

import run_glue_demo as run_gluedemo_script

@pytest.mark.integ
def test_run_integ(monkeypatch):
   # cf_client = boto3.client("cloudformation")
   # s3_resource = boto3.resource("s3")
    glue_client = boto3.client("glue")
    # iam_resource = boto3.resource("iam")
    inputs = ["cards233", "y","cards233", "y" ]
    monkeypatch.setattr("builtins.input", lambda x: inputs.pop(0))
  
    with patch("builtins.print") as mock_print:
        args = [ run_gluedemo_script.parse_args(["deploy"]), run_gluedemo_script.parse_args(["destroy"]) ]
       # args = [ run_gluedemo_script.parse_args(["destroy"]) ]
        monkeypatch.setattr(run_gluedemo_script, "parse_args", lambda x: args.pop(0))
        run_gluedemo_script.main()
        crawler = glue_client.get_crawler(Name="cards233-gluedemo-crawler")
        assert crawler['Crawler']['Name'] == 'cards233-gluedemo-crawler'
        database = glue_client.get_database(Name='cards233-gluedemo-database')
        assert database['Database']['Name'] == 'cards233-gluedemo-database'
        run_gluedemo_script.main()
        with pytest.raises(ClientError) as exc_info:
            glue_client.get_crawler(Name="cards233-gluedemo-crawler")
        assert exc_info.value.response["Error"]["Code"] == "EntityNotFoundException"
        with pytest.raises(ClientError) as exc_info:
            glue_client.get_database(Name='cards233-gluedemo-database')
        assert exc_info.value.response["Error"]["Code"] == "EntityNotFoundException"

