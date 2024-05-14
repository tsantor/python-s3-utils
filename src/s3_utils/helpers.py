from boto3.session import Session


def list_buckets_by_name(session: Session) -> list:
    """
    List all S3 buckets in the AWS account associated with the given session.

    Args:
        session (Session): A session represents a connection to AWS.

    Returns:
        list: A list of names of all S3 buckets.
    """
    s3 = session.client("s3")
    response = s3.list_buckets()
    return [bucket["Name"] for bucket in response["Buckets"]]
