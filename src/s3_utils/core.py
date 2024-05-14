import concurrent.futures
import logging
from pathlib import Path
from typing import Optional

from boto3.session import Session
from botocore.exceptions import BotoCoreError
from botocore.exceptions import ClientError

from .exceptions import S3KeyNotFoundError

logger = logging.getLogger(__name__)


class S3Bucket:
    """Wrapper class for simplified S3 bucket operations."""

    def __init__(self, session: Session, bucket: str):
        self.client = session.client("s3")
        self.bucket = bucket

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
            self.client.head_object(Bucket=self.bucket, Key=key_name)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            raise

    def list_objects_by_prefix(self, prefix: str = "") -> list[dict]:
        """
        Lists all objects in an S3 bucket that have a certain prefix.

        Args:
            prefix (str): The prefix to filter objects by.

        Returns:
            list[dict]: A list of dictionaries representing the objects. Each
                        dictionary contains information about an object, such as
                        its key and last modified date.
        """
        response = self.client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
        return response.get("Contents", [])

    def upload_file(self, file_name: str, key_name: Optional[str] = None) -> None:  # noqa: UP007
        """
        Uploads a file to S3 bucket.

        :param file_name: The name of the file to upload.
        :param key_name: The name of the object in S3. If not specified, file_name is used.
        """
        # If S3 key_name was not specified, use file_name
        if key_name is None:
            key_name = Path(file_name).name

        self.client.upload_file(file_name, self.bucket, key_name)

    def upload_files(self, directory_path: str, s3_folder: str) -> None:
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

    # def upload_files(self, local_path: str, s3_folder: str):
    #     """
    #     Uploads files from a local directory to an S3 bucket and returns a list of the uploaded file paths.

    #     Args:
    #         local_path (str): The local path to the directory containing the files to upload.
    #         s3_folder (str): The S3 folder to upload the files to.
    #     """
    #     s3 = session.resource("s3")
    #     bucket = s3.Bucket(self.bucket)
    #     local_path = Path(local_path)

    #     for file in local_path.rglob("*"):
    #         if file.is_file():
    #             with file.open("rb") as data:
    #                 key = Path(s3_folder) / file.relative_to(local_path)
    #                 # logger.debug(f"Uploading {file} to {bucket_name}/{key}")
    #                 bucket.put_object(Key=str(key), Body=data)

    def download_file(self, key_name, local_path=None):
        """Download a file to an S3 bucket

        :param key_name: File to download
        :param local_path: Local file name. If not specified then key_name is used
        :return: True if file was downloaded, else False
        """

        if not self.file_exists(key_name):
            msg = f'Key "{key_name}" not found.'
            raise S3KeyNotFoundError(msg)

        # If local_path was not specified, use key_name
        if local_path is None:
            local_path = Path(key_name).expanduser()

        # Create any parent dirs if need be
        parent_dir = Path(local_path).expanduser().parent
        if not parent_dir.is_dir() and str(parent_dir) != ".":
            parent_dir.mkdir(parents=True, exist_ok=True)

        self.client.download_file(self.bucket, key_name, str(local_path))

    def download_directory(self, dir_name, target_dir="~/Downloads"):
        """Download all files in a directory.

        :param dir_name: Directory name
        :param target_dir: Target directory name
        :return: None
        """

        if not dir_name.endswith("/"):
            dir_name = f"{dir_name}/"

        if not self.exists(dir_name):
            msg = f'Key "{dir_name}" not found.'
            raise S3KeyNotFoundError(msg)

        if isinstance(target_dir, str):
            target_dir = Path(target_dir).expanduser()

        # Download the files
        objs = self.client.list_objects_v2(Bucket=self.bucket, Prefix=dir_name)
        for obj in objs["Contents"]:
            file_name = obj["Key"]
            if file_name != dir_name:
                self.download(file_name, target_dir / Path(file_name))

    def delete_file(self, key_name: str) -> None:
        """
        Deletes a file from S3 bucket.

        :param file_name: The name of the file to delete.
        """
        self.client.delete_object(Bucket=self.bucket, Key=key_name)

    def delete_files(self, keys: list[dict]) -> dict:
        """
        Deletes specified objects from an S3 bucket.

        Parameters:
        keys (list): A list of dictionaries, each containing the key of an
                     object to delete. Each dictionary should be of the form
                     {"Key": "object_key"}.

        Returns:
        dict: A summary of the deletion operation.
        """
        deleted_objects = []
        not_deleted_objects = []

        response = self.client.delete_objects(
            Bucket=self.bucket, Delete={"Objects": keys}
        )

        # Check for errors
        errors = response.get("Errors", [])
        not_deleted_objects = [error["Key"] for error in errors]

        # Determine which objects were deleted
        deleted_objects = [obj for obj in keys if obj["Key"] not in not_deleted_objects]

        return {
            "deleted_objects": deleted_objects,
            "deleted_count": len(deleted_objects),
            "not_deleted_objects": not_deleted_objects,
            "not_deleted_count": len(not_deleted_objects),
        }
