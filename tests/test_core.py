import pytest
from botocore.exceptions import ClientError


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


def test_list_objects_recursive(s3bucket, prefix, s3_setup):
    """Test that the function returns the correct objects."""
    s3_resource = s3_setup["s3_resource"]
    bucket_name = s3_setup["bucket_name"]

    # Put objects in the bucket
    total_objects = 2000
    for i in range(total_objects):
        s3_resource.Object(bucket_name, f"{prefix}/test_object_{i}").put(
            Body=b"content"
        )
    # s3_resource.Object(bucket_name, f"{prefix}/test_object").put(Body=b"content")
    s3_resource.Object(bucket_name, "root_object").put(Body=b"content")

    # Test with a prefix
    objects = list(s3bucket.list_objects_recursive(prefix))
    assert len(objects) == total_objects

    # Test it without the prefix and not recursive (only 1000 can be listed)
    objects = list(s3bucket.list_objects())
    assert len(objects) == 1000  # noqa: PLR2004


def test_download_file(s3bucket, s3_setup, tmp_path):
    """Test that the function downloads the file correctly."""
    bucket_name = s3_setup["bucket_name"]
    s3_client = s3_setup["s3_client"]

    # Put object in the bucket
    s3_client.put_object(Bucket=bucket_name, Key="mydir/mykey", Body="mybody")

    # Test the function
    local_file = s3bucket.download_file("mydir/mykey", str(tmp_path))
    assert local_file.is_file()
    local_file.unlink()

    # Test with non-existent key
    with pytest.raises(ClientError):
        s3bucket.download_file("non_existent_key", str(tmp_path))

    # Test with non-existent local directory
    local_file = s3bucket.download_file("mydir/mykey")
    assert local_file.is_file()
    local_file.unlink()


def test_upload_file(s3bucket, tmp_path):
    """Test that the function uploads the file correctly."""
    temp_file = tmp_path / "mytempfile"
    temp_file.write_text("mybody")

    # Test the function
    s3bucket.upload_file(temp_file, "mykey")

    # Check that the file was uploaded
    s3bucket.file_exists("mykey")

    # Test with no key_name
    s3bucket.upload_file(temp_file)
    s3bucket.file_exists(str(temp_file))


def test_upload_files(s3bucket, tmp_path):
    """Test that the function uploads the files correctly."""
    temp_dir = tmp_path / "mytempdir"
    (temp_dir / "file1").write_text("mybody")
    (temp_dir / "file2").write_text("mybody")

    # Test the function
    s3bucket.upload_files(temp_dir)

    # Check that the files were uploaded
    assert s3bucket.file_exists("file1")
    assert s3bucket.file_exists("file2")


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
