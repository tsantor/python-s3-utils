from pathlib import Path

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws
from s3_utils.core import S3Bucket
from s3_utils.helpers import list_buckets


@pytest.fixture(scope="module")
@mock_aws
def session():
    return boto3.Session(region_name="us-east-1")


@pytest.fixture(scope="module")
def bucket_name():
    return "test-bucket"


@pytest.fixture(scope="module")
def prefix():
    return "test-prefix"


@pytest.fixture(scope="module")
def s3bucket(session, bucket_name) -> S3Bucket:
    return S3Bucket(session, bucket_name)


@pytest.fixture(scope="module")
def s3_setup(session, bucket_name):
    # Start the mock S3 service
    mock_aws().start()

    s3_resource = session.resource("s3")
    s3_resource.create_bucket(Bucket=bucket_name)

    # Yield the necessary objects for the tests
    yield {
        "session": session,
        "s3_resource": s3_resource,
        "s3_client": boto3.client("s3", region_name="us-east-1"),
        "bucket_name": bucket_name,
    }

    # Cleanup after tests
    mock_aws().stop()


def test_list_buckets(s3_setup):
    """Test that the function returns the correct buckets."""
    session = s3_setup["session"]
    s3_resource = s3_setup["s3_resource"]

    # Create some buckets
    s3_resource.create_bucket(Bucket="mybucket1")
    s3_resource.create_bucket(Bucket="mybucket2")

    buckets = list_buckets(session)

    assert isinstance(buckets, list)
    assert set(buckets) == {"mybucket1", "mybucket2", "test-bucket"}


def test_list_objects_by_prefix(s3bucket, prefix, s3_setup):
    """Test that the function returns the correct objects."""
    s3_resource = s3_setup["s3_resource"]
    bucket_name = s3_setup["bucket_name"]

    # Put objects in the bucket
    s3_resource.Object(bucket_name, f"{prefix}/test_object").put(Body=b"content")
    s3_resource.Object(bucket_name, "root_object").put(Body=b"content")

    # Test with a prefix
    objects = s3bucket.list_objects_by_prefix(prefix)
    assert len(objects) == 1
    assert objects[0]["Key"] == f"{prefix}/test_object"

    # Test it without the prefix
    objects = s3bucket.list_objects_by_prefix()
    keys = [f"{prefix}/test_object", "root_object"]
    object_keys = [obj["Key"] for obj in objects]
    for key in keys:
        assert key in object_keys


def test_file_exists(s3bucket, s3_setup):
    """Test that the function returns the correct value."""
    s3_resource = s3_setup["s3_resource"]
    bucket_name = s3_setup["bucket_name"]

    # Put object in the bucket
    key_name = "test_key"
    s3_resource.Object(bucket_name, key_name).put(Body=b"content")

    # Test the function
    assert s3bucket.file_exists(key_name)
    assert not s3bucket.file_exists("non_existent_key")


def test_download_file(s3bucket, s3_setup):
    """Test that the function downloads the file correctly."""
    bucket_name = s3_setup["bucket_name"]
    s3_client = s3_setup["s3_client"]

    # Put object in the bucket
    s3_client.put_object(Bucket=bucket_name, Key="mykey", Body="mybody")

    # Test the function
    s3bucket.download_file("mykey", "mylocalfile")
    assert Path("mylocalfile").expanduser().is_file()
    Path("mylocalfile").expanduser().unlink()


def test_upload_file(s3bucket, s3_setup):
    """Test that the function uploads the file correctly."""
    bucket_name = s3_setup["bucket_name"]
    s3_client = s3_setup["s3_client"]

    # Create a local file
    with Path("mylocalfile").open("w", encoding="utf-8") as f:
        f.write("mybody")

    # Test the function
    if s3bucket.upload_file("mylocalfile", "mykey"):
        # Check that the file was uploaded
        obj = s3_client.get_object(Bucket=bucket_name, Key="mykey")
        body = obj["Body"].read()

        assert body == b"mybody"

    Path("mylocalfile").unlink()


def test_delete_file(s3bucket, s3_setup):
    """Test that the function deletes the object correctly."""
    bucket_name = s3_setup["bucket_name"]
    s3_client = s3_setup["s3_client"]

    # Put object in the bucket
    s3_client.put_object(Bucket=bucket_name, Key="mykey", Body="mybody")

    # Test the function
    s3bucket.delete_file("mykey")

    # Ensure the object was deleted
    with pytest.raises(ClientError):
        s3_client.get_object(Bucket=bucket_name, Key="mykey")


def test_delete_objects(s3bucket, s3_setup):
    """Test that the function deletes the objects correctly."""
    bucket_name = s3_setup["bucket_name"]
    s3_client = s3_setup["s3_client"]

    # Put objects in the bucket
    s3_client.put_object(Bucket=bucket_name, Key="mykey1", Body="mybody")
    s3_client.put_object(Bucket=bucket_name, Key="mykey2", Body="mybody")

    # Test the function
    result = s3bucket.delete_files([{"Key": "mykey1"}, {"Key": "mykey2"}])
    assert result["deleted_count"] == 2
    assert result["not_deleted_count"] == 0

    with pytest.raises(ClientError):
        s3_client.get_object(Bucket=bucket_name, Key="mykey1")

    with pytest.raises(ClientError):
        s3_client.get_object(Bucket=bucket_name, Key="mykey2")
