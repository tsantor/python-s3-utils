import concurrent.futures
import logging
from pathlib import Path
from typing import Optional

from boto3.session import Session
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class S3Bucket:
    """Wrapper class for simplified S3 bucket operations."""

    def __init__(self, session: Session, bucket_name: str):
        self.client = session.client("s3")
        self.bucket_name = bucket_name

    def file_exists(self, key_name: str) -> bool:
        """
        Checks if a file exists in an S3 bucket.

        Args:
            key_name (str): The name of the S3 key.

        Returns:
            bool: True if the file exists, False otherwise.

        Raises:
            ClientError: An error occurred when trying to access the S3 bucket.
        """
        try:
            self.client.head_object(Bucket=self.bucket_name, Key=key_name)
            return True
        except ClientError as e:
            # If a 404 error is raised, the object does not exist
            if e.response["Error"]["Code"] == "404":
                return False
            # If another error is raised, re-raise it
            raise  # pragma: no cover

    def list_objects_recursive(self, prefix: str = ""):
        """
        Lists all objects in an S3 bucket that have a certain prefix.

        Args:
            prefix (str, optional): The prefix to filter objects in the S3
                                    bucket.Defaults to an empty string, which
                                    means all objects in the bucket.

        Returns:
            Generator[Dict[str, str]]: A generator that yields dictionaries
                                       containing information about each object
                                       in the S3 bucket that matches the prefix.
        """
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
            yield from page.get("Contents", [])

    def list_objects(self, prefix: str = "") -> list[dict]:
        """
        Lists objects in an S3 bucket that have a certain prefix.

        Args:
            prefix (str): The prefix to filter objects by.

        Returns:
            list[dict]: A list of dictionaries representing the objects. Each
                        dictionary contains information about an object, such
                        as its key and last modified date.
        """
        response = self.client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
        return response.get("Contents", [])

    def upload_file(self, file_name: str, key_name: Optional[str] = None) -> str:  # noqa: UP007
        """
        Uploads a file to S3 bucket.

        Args:
            file_name (str): The name of the file to upload.
            key_name(str): The name of the object in S3. If not specified,
                           file_name is used.

        Returns:
            str: The key of the uploaded file in the S3 bucket.
        """
        # If S3 key_name was not specified, use file_name
        if key_name is None:
            key_name = Path(file_name).name

        self.client.upload_file(file_name, self.bucket_name, key_name)
        return key_name

    def upload_files(self, directory_path: str) -> None:
        """
        Uploads all files from a given directory to the S3 bucket.

        :param directory_path: The path of the directory to upload files from.
        """
        # Get a list of all files in the directory
        directory = Path(directory_path)
        files = [f for f in directory.iterdir() if f.is_file()]

        # Use a ThreadPoolExecutor to upload files in parallel
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for file_name in files:
                executor.submit(self.upload_file, str(file_name))

    def download_file(self, key_name: str, local_dir: str = "") -> Path:
        """
        Downloads a file from an S3 bucket.

        Args:
            key_name (str): The key of the file in the S3 bucket.
            local_dir (str): The local directory to download the file to..

        Returns:
            Path: The local path where the file was downloaded.
        """
        local_path = Path(local_dir) / key_name
        local_path.parent.mkdir(parents=True, exist_ok=True)
        self.client.download_file(self.bucket_name, key_name, str(local_path))
        return local_path

    def delete_file(self, key_name: str) -> None:
        """
        Deletes a file from S3 bucket.

        Args:
            key_name (str): The name of the file to delete.
        """
        self.client.delete_object(Bucket=self.bucket_name, Key=key_name)

    def delete_files(self, keys: list[str]) -> dict:
        """
        Deletes specified objects from an S3 bucket.

        Parameters:
        keys (list): A list of key names.

        Returns:
        dict: A summary of the deletion operation.
        """
        # Prepare the delete request
        delete_request = {
            "Objects": [{"Key": key} for key in keys],
            "Quiet": False,
        }

        response = self.client.delete_objects(
            Bucket=self.bucket_name, Delete=delete_request
        )

        # Process the response
        deleted = response.get("Deleted", [])
        errors = response.get("Errors", [])

        deleted = [obj["Key"] for obj in deleted]
        errors = [obj["Key"] for obj in errors]

        return {
            "deleted": deleted,
            "deleted_count": len(deleted),
            "errors": errors,
            "error_count": len(errors),
        }
