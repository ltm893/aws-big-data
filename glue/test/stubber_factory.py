# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
A factory function that returns the stubber for an AWS service, based on the
name of the service that is used by Boto 3.

This factory is used by the make_stubber fixture found in the set of common fixtures.
"""

from cloudformation_stubber import CloudFormationStubber
from glue_stubber import GlueStubber
from s3_stubber import S3Stubber
from iam_stubber import IamStubber



class StubberFactoryNotImplemented(Exception):
    pass


def stubber_factory(service_name):
    if service_name == "cloudformation":
        return CloudFormationStubber
    elif service_name == "glue":
        return GlueStubber
    elif service_name == "iam":
        return IamStubber
    elif service_name == "s3":
        return S3Stubber
    else:
        raise StubberFactoryNotImplemented(
            "If you see this exception, it probably means that you forgot to add "
            "a new stubber to stubber_factory.py."
        )
