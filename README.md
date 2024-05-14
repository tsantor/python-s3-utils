# Python S3 Utils

![Coverage](https://img.shields.io/badge/coverage-100%25-brightgreen)

## Overview

Some simple wrapper functions around boto3 functionality for common interactions with S3.

## Installation

Install Python S3 Utils:

```bash
python3 -m pip install python-s3-utils
```

## Usage
```python
import s3_utils

session = Session(
    aws_access_key_id="AWS_ACCESS_KEY_ID",
    aws_secret_access_key="AWS_SECRET_ACCESS_KEY",
)

list_buckets(session)
```

Provides the following helper functions:
- list_buckets
- list_objects_by_prefix
- file_exists
- download_file
- download_files
- upload_file
- upload_files
- delete_object
- delete_objects

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

## Deploying

```bash
# Publish to PyPI Test before the live PyPi
make release_test
make release
```

## Issues

If you experience any issues, please create an [issue](https://github.com/tsantor/python-s3-utils/issues) on Github.
