# Python S3 Utils

![Coverage](https://img.shields.io/badge/coverage-100%25-brightgreen)

## Overview

Wrapper around boto3 functionality for common interactions with S3. It may be a bit overkill, but there are some minor quality of life improvements.

## Installation

Install Python S3 Utils:

```bash
python3 -m pip install python-s3-utils
```

## Usage
```python
import boto3
from s3_utils import S3Bucket

session = boto3.session.Session(
    aws_access_key_id="AWS_ACCESS_KEY_ID",
    aws_secret_access_key="AWS_SECRET_ACCESS_KEY",
)

s3bucket = S3Bucket(session, "bucket-name")

# Returns True/False
s3bucket.file_exists('key-name')

# Returns a generator of all objects in the bucket, does not have a 1000 object limit like `list_objects`
s3bucket.list_objects_recursive()

# File name becomes the key name if key_name not provided
s3bucket.upload_file("path/filename.jpg", key_name=None)

# Upload all files in a directory
s3bucket.upload_files("path/")

# File would be downloaded to target_dir/prefix/filename.jpg
s3bucket.download_file("prefix/filename.jpg", "target_dir")

# Returns a dict summary of the operation
s3bucket.delete_files(["path/filename.jpg", "key-name"])
```

## Development
To get a list of all commands with descriptions simply run `make`.

```bash
make env
make pip_install
make pip_install_editable
```

## Testing

```bash
make pytest
make coverage
make open_coverage
```

## Issues

If you experience any issues, please create an [issue](https://github.com/tsantor/python-s3-utils/issues) on GitHub.
