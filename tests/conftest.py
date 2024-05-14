import boto3
import pytest
from moto import mock_aws
from s3_utils.core import S3Bucket


@pytest.fixture(scope="module")
@mock_aws
def session():
    return boto3.Session(region_name="us-east-1")


@pytest.fixture(scope="module")
def bucket_name():
    return "test-bucket"


@pytest.fixture()
def prefix():
    return "test-prefix"


@pytest.fixture(scope="module")
def s3bucket(session, bucket_name) -> S3Bucket:
    return S3Bucket(session, bucket_name)


@pytest.fixture(scope="module")
def s3_setup(session, bucket_name):
    """Fixture to setup the S3 bucket for testing."""
    # Start the mock S3 service
    mock_aws().start()

    s3_resource = session.resource("s3")
    s3_resource.create_bucket(Bucket=bucket_name)

    # Yield the necessary objects for the tests
    yield {
        "session": session,
        "s3_resource": s3_resource,
        "s3_client": session.client("s3"),
        "bucket_name": bucket_name,
    }

    # Cleanup after tests
    mock_aws().stop()
