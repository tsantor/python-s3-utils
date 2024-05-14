from pathlib import Path

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws
from s3_utils.core import S3Bucket
from s3_utils.helpers import list_buckets


@pytest.fixture()
def bucket_name():
    """Return the name of the test bucket."""
    return "test_bucket"


@pytest.fixture()
def prefix():
    """Return the prefix for the test objects."""
    return "test_prefix"


@pytest.fixture()
def session():
    """Return a Boto3 session."""
    return boto3.Session(region_name="us-east-1")


@pytest.fixture()
def client():
    return boto3.client("s3", region_name="us-east-1")


@pytest.fixture()
def s3bucket(session, bucket_name) -> S3Bucket:
    return S3Bucket(session, bucket_name)


@mock_aws
def test_list_buckets(session):
    """Test that the function returns the correct buckets."""
    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket="mybucket1")
    conn.create_bucket(Bucket="mybucket2")

    buckets = list_buckets(session)

    assert isinstance(buckets, list)
    assert set(buckets) == {"mybucket1", "mybucket2"}


@mock_aws
def test_list_objects_by_prefix(s3bucket, prefix, session, bucket_name):
    """Test that the function returns the correct objects."""
    # Create a mock S3 bucket and add an object with the prefix
    s3 = session.resource("s3")
    s3.create_bucket(Bucket=bucket_name)
    s3.Object(bucket_name, f"{prefix}/test_object").put(Body=b"content")
    s3.Object(bucket_name, "root_object").put(Body=b"content")

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


@mock_aws
def test_file_exists(s3bucket, session, bucket_name):
    """Test that the function returns the correct value."""
    key_name = "test_key"

    # Create a mock S3 bucket and add an object
    s3 = session.resource("s3")
    s3.create_bucket(Bucket=bucket_name)
    s3.Object(bucket_name, key_name).put(Body=b"content")

    # Test the function
    assert s3bucket.file_exists(key_name)
    assert not s3bucket.file_exists("non_existent_key")


@mock_aws
def test_download_file(s3bucket, session, bucket_name):
    """Test that the function downloads the file correctly."""
    s3 = session.resource("s3")
    s3.create_bucket(Bucket=bucket_name)

    s3 = boto3.client("s3", region_name="us-east-1")
    s3.put_object(Bucket=bucket_name, Key="mykey", Body="mybody")

    s3bucket.download_file("mykey", "mylocalfile")

    assert Path("mylocalfile").expanduser().is_file()

    Path("mylocalfile").expanduser().unlink()


@mock_aws
def test_upload_file(s3bucket, session, bucket_name):
    """Test that the function uploads the file correctly."""
    s3 = session.resource("s3")
    s3.create_bucket(Bucket=bucket_name)

    # Create a local file
    with Path("mylocalfile").open("w", encoding="utf-8") as f:
        f.write("mybody")

    if s3bucket.upload_file("mylocalfile", "mykey"):
        # Check that the file was uploaded
        s3 = boto3.client("s3", region_name="us-east-1")
        obj = s3.get_object(Bucket=bucket_name, Key="mykey")
        body = obj["Body"].read()

        assert body == b"mybody"

    Path("mylocalfile").unlink()


@mock_aws
def test_delete_file(s3bucket, bucket_name):
    """Test that the function deletes the object correctly."""
    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket=bucket_name)

    s3 = boto3.client("s3", region_name="us-east-1")
    s3.put_object(Bucket=bucket_name, Key="mykey", Body="mybody")

    s3bucket.delete_file("mykey")

    with pytest.raises(ClientError):
        s3.get_object(Bucket=bucket_name, Key="mykey")


@mock_aws
def test_delete_objects(s3bucket, bucket_name):
    """Test that the function deletes the objects correctly."""
    deleted_count = 2

    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket=bucket_name)

    s3 = boto3.client("s3", region_name="us-east-1")
    s3.put_object(Bucket=bucket_name, Key="mykey1", Body="mybody")
    s3.put_object(Bucket=bucket_name, Key="mykey2", Body="mybody")

    result = s3bucket.delete_files([{"Key": "mykey1"}, {"Key": "mykey2"}])

    assert result["deleted_count"] == deleted_count
    assert result["not_deleted_count"] == 0

    with pytest.raises(ClientError):
        s3.get_object(Bucket=bucket_name, Key="mykey1")

    with pytest.raises(ClientError):
        s3.get_object(Bucket=bucket_name, Key="mykey2")
