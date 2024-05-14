from s3_utils.helpers import list_buckets_by_name


def test_list_buckets_by_name(s3_setup):
    """Test that the function returns the correct buckets."""
    session = s3_setup["session"]
    s3_resource = s3_setup["s3_resource"]

    # Create some buckets
    s3_resource.create_bucket(Bucket="mybucket1")
    s3_resource.create_bucket(Bucket="mybucket2")

    buckets = list_buckets_by_name(session)

    assert isinstance(buckets, list)
    assert set(buckets) == {"mybucket1", "mybucket2", "test-bucket"}
