from boto3 import Session
from utils import read_config
import os
from rich import print

CONFIG_FILE = "config.yaml"


def create_session(config=read_config()):
    aws_access_key_id, aws_secret_access_key = map(config['yandex']['cloud'].get, ['aws_access_key_id', 'aws_secret_access_key'])
    return Session().client(
        service_name='s3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        endpoint_url='https://storage.yandexcloud.net'
    )

def upload_file(path, bucket, key, session, skip_if_exists=False):
    if not (skip_if_exists and session.list_objects_v2(Bucket=bucket, Prefix=key, MaxKeys=1).get("Contents", [])):
        session.upload_file(
            path,
            bucket,
            key
        )
    
    return f"{session._endpoint.host}/{bucket}/{key}"

def download(bucket, download_dir, prefix=''):
    s3 = create_session()

    # List and download all objects under prefix
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            local_path = os.path.join(download_dir, os.path.relpath(key, prefix))
            # if not os.path.exists(local_path):
            #     print(f"Downloading {key} to {local_path}")
            #     s3.download_file(bucket, key, local_path)
            yield local_path
