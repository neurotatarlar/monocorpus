import os.path

import typer
import yaml
from boto3 import Session
from file_utils import calculate_md5, get_path_in_workdir, read_config
from rich.progress import track
import multiprocessing
from urllib.parse import urlparse

from consts import Dirs
from file_utils import read_config
from concurrent.futures import ThreadPoolExecutor
from concurrent import futures


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


def list_files(bucket: str, folder: str = "", session=create_session()):
    """
    List files in the S3 bucket in the specified folder
    """
    paginator = session.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=folder)
    return {
        obj['Key']: obj['ETag'].strip('"')
        for page in pages
        for obj in page['Contents']
    }


def download_in_parallel(key_to_md5, func, bucket, download_folder, session = create_session()):
    """
    Download files in parallel
    :param key_to_md5: dictionary with keys and expected md5 hashes
    :param func: function to download the file
    :param bucket: S3 bucket
    :param download_folder: local folder to download the files to
    :param session: boto3 session
    :return: generator of downloaded files with path_to_file as key and expected md5 as value
    """
    with ThreadPoolExecutor(max_workers=8) as executor:
        future_to_key = {executor.submit(func, bucket=bucket, key=key, expected_md5=md5, download_folder=download_folder, session=session): key for key, md5 in key_to_md5.items()}

        for future in track(futures.as_completed(future_to_key), description="Downloading files...", total=len(future_to_key)):
            key = future_to_key[future]
            if exception := future.exception():
                executor.shutdown(wait=True, cancel_futures=True)
                print(f"Failed to download {key}: {exception}")
                raise typer.Abort()

            yield future.result()


def download_annotation(bucket, key, expected_md5, download_folder, session):
    output_file = os.path.join(download_folder, key)
    if not (os.path.exists(output_file) and expected_md5 == calculate_md5(output_file)):
        session.download_file(
            bucket,
            key,
            output_file
        )
    return output_file


def download_annotation_summaries(bucket: str, keys, session=create_session()):
    """
    Download annotation results from the S3 bucket if not already downloaded
    """
    downloaded_files = {}
    download_folder = get_path_in_workdir(Dirs.ANNOTATION_RESULTS)
    for md5 in track(keys, description=f"Downloading annotation results from the `{bucket}`..."):
        file_name = f"{md5}.json"
        output_file = os.path.join(download_folder, file_name)
        if not os.path.exists(output_file):
            print(f"Downloading {file_name} to {output_file}")
            session.download_file(
                bucket,
                file_name,
                output_file
            )
        downloaded_files[md5] = output_file

    return downloaded_files



