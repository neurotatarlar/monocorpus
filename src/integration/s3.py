import os.path

import yaml
from boto3 import Session
from file_utils import calculate_md5, get_path_in_workdir, read_config
from rich.progress import track
import multiprocessing
from urllib.parse import urlparse

from consts import Dirs

CONFIG_FILE = "config.yaml"


def create_session(config=read_config()):
    aws_access_key_id, aws_secret_access_key = map(config['yc'].get, ['aws_access_key_id', 'aws_secret_access_key'])
    session = Session().client(
        service_name='s3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        endpoint_url='https://storage.yandexcloud.net'
    )
    return session


def upload_files_to_s3(files: [], bucket_provider, session=create_session(), **kwargs):
    """
    Uploads files to the S3 bucket in the specified folder
    """
    config = read_config()
    bucket = bucket_provider(config)

    # get all files in the remote folder
    remote_files = {
        os.path.split(f['Key'])[-1]: f['ETag'].strip('"')
        for f
        in session.list_objects(Bucket=bucket, **kwargs).get('Contents', [])
        # filter out directories
        if not f['Key'].endswith("/")
    }

    upload_result = {}
    for l_file in track(files, description=f"Uploading files to the `{bucket}`..."):
        l_file_name = os.path.split(l_file)[-1]
        file_key = f"{kwargs['Prefix']}/{l_file_name}" if kwargs.get('Prefix') else l_file_name

        # check if the file with the same name exists on S3 and has the same digest
        if l_file_name not in remote_files or remote_files[l_file_name] != calculate_md5(l_file):
            session.upload_file(
                l_file,
                bucket,
                file_key
            )
        upload_result[l_file] = f"s3://{bucket}/{file_key}"

    return upload_result


def list_files(bucket: str, folder: str = "", session= create_session()):
    """
    List files in the S3 bucket in the specified folder
    """
    return {
        f['Key']: f['ETag'].strip('"')
        for f
        in session.list_objects(Bucket=bucket, Prefix=folder).get('Contents', [])
    }


def download_annotations(bucket: str, keys:[], session=create_session()):
    """
    Download annotations from the S3 bucket
    """
    downloaded_annotations = []
    download_folder = get_path_in_workdir(Dirs.ANNOTATIONS)

    for key, r_md5 in track(keys, description=f"Downloading annotations from the `{bucket}`..."):
        output_file = os.path.join(download_folder, key)
        if not (os.path.exists(output_file) and r_md5 == calculate_md5(output_file)):
            session.download_file(
                bucket,
                key,
                output_file
            )
        downloaded_annotations.append((output_file, r_md5))

    return downloaded_annotations


def download_annotation_summaries(bucket: str, keys, session=create_session()):
    """
    Download annotation results from the S3 bucket
    """
    downloaded_files = {}
    download_folder = get_path_in_workdir(Dirs.ANNOTATION_RESULTS)
    for md5, link in track(keys.items(), description=f"Downloading annotation results from the `{bucket}`..."):
        output_file = os.path.join(download_folder, f"{md5}.json")
        res = urlparse(link)
        if not os.path.exists(output_file):
            print(f"Downloading {link} to {output_file}")
            session.download_file(
                res.netloc,
                res.path.lstrip("/"),
                output_file
            )
        downloaded_files[md5] = output_file

    return downloaded_files



