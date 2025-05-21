from boto3 import Session
from utils import read_config

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
    # if not (skip_if_exists and session.list_objects_v2(Bucket=bucket, Prefix=key, MaxKeys=1).get("Contents", [])):
    #     session.upload_file(
    #         path,
    #         bucket,
    #         key
    #     )
    
    return f"{session._endpoint.host}/{bucket}/{key}"
