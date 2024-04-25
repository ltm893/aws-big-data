import boto3
import os
import pytest

from moto import mock_aws


@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ['MOTO_ACCOUNT_ID'] = '111111111111'


@pytest.fixture
def s3_resource(aws_credentials):
    with mock_aws():
        conn = boto3.resource("s3", region_name="us-east-2")
        yield conn


@pytest.fixture
def glue_client(aws_credentials):
    with mock_aws():
        conn = boto3.client("glue", region_name="us-east-2")
        yield conn
