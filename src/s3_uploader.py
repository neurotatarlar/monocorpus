import os.path

import yaml
from boto3 import Session
from rich.progress import track

from file_utils import calculate_md5, get_path_in_workdir

CONFIG_FILE = "config.yaml"


def upload_files_to_s3(files: [], folder: str):
    """
    Uploads files to the S3 bucket in the specified folder
    """
    with open(get_path_in_workdir(CONFIG_FILE, prefix="."), 'r') as file:
        config = yaml.safe_load(file)

    s3 = _create_session(config)
    bucket = config['yc']['bucket']

    # get all files in the remote folder
    remote_files = {
        os.path.split(f['Key'])[-1]: f['ETag'].strip('"')
        for f
        in s3.list_objects(Bucket=bucket, Prefix=folder).get('Contents', [])
        # filter out directories
        if not f['Key'].endswith("/")
    }

    upload_result = {}
    for l_file in track(files, description=f"Uploading files to the `{bucket}/{folder}`..."):
        l_file_name = os.path.split(l_file)[-1]

        # check if the file with the same name exists on S3 and has the same digest
        if l_file_name not in remote_files or remote_files[l_file_name] != calculate_md5(l_file):
            s3.upload_file(
                l_file,
                bucket,
                f"{folder}/{l_file_name}"
            )
        upload_result[l_file] = f"s3://{bucket}/{folder}/{l_file_name}"

    return upload_result


def _create_session(config):
    aws_access_key_id, aws_secret_access_key = map(config['yc'].get, ['aws_access_key_id', 'aws_secret_access_key'])
    return Session().client(
        service_name='s3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        endpoint_url='https://storage.yandexcloud.net'
    )
