from boto3.session import Session


def list_buckets(session: Session) -> list:
    """
    List all S3 buckets in the AWS account associated with the given session.

    Args:
        session (boto3.Session): A session represents a connection to AWS.
                                 This can be a default session or a session
                                 with specific AWS credentials.

    Returns:
        list: A list of names of all S3 buckets.
    """
    s3 = session.client("s3")
    response = s3.list_buckets()
    return [bucket["Name"] for bucket in response["Buckets"]]
