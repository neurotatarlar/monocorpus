import os.path
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor

import typer
from boto3 import Session
from dirs import Dirs
from utils import read_config
from rich.progress import track

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
