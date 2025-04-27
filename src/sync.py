from utils import read_config, get_in_workdir
from gsheets import get_all_md5s, upsert, find_by_file_name, remove_file
from yadisk_client import YaDisk
from rich import print
from monocorpus_models import Document
from s3 import upload_file, create_session
import os
from rich.progress import track
import zipfile
import json

not_document_types = [
    'application/vnd.android.package-archive',
    'image/jpeg',
    'application/x-zip-compressed',
    'application/zip'
    'application/octet',
    'application/octet-stream',
    'text/x-python'
    'application/x-gzip',
    'text-html',
    'application/x-rar',
    'application/x-download',
    "application/json",
    'audio/mpeg'
]

def sync():
    """
    Syncs files from Yandex Disk to Google Sheets.
    """
    all_md5s = get_all_md5s()
    config = read_config()
    dirs_to_visit = [config['yandex']['disk']['entry_point']]
    skipped_by_mime_type_files = []
    s3client = create_session(config)
    upstream_meta = _lookup_upstream_metadata(s3client, config)
    with YaDisk(config['yandex']['disk']['oauth_token']) as client:
        while dirs_to_visit:
            current_dir = dirs_to_visit.pop(0)
            print(f"Visiting: '{current_dir}'")
            listing = client.listdir(current_dir, max_items=None, fields=['type', 'path', 'mime_type', 'md5', 'public_key', 'public_url', 'resource_id', 'name'])
            for resource in listing:
                if resource.type == 'dir':
                    dirs_to_visit.append(resource.path)
                elif resource.type == 'file':
                    _process_file(client, resource, all_md5s, skipped_by_mime_type_files, upstream_meta.get(resource.md5, None))
    
    print("Skipped by MIME type files:")
    for s in skipped_by_mime_type_files:
        print(s)


def _process_file(ya_client, file, all_md5s, skipped_by_mime_type_files, upstream_meta):

    if file.mime_type in not_document_types:
        print(f"Skipping file: '{file.path}' of type '{file.mime_type}'")
        skipped_by_mime_type_files.append((file.mime_type, file.public_url, file.path))
        return
    
    ya_public_key = file.public_key
    ya_public_url = file.public_url
    if not (ya_public_key and ya_public_url):
        ya_public_key, ya_public_url = publish_file(ya_client, file.path)
    
    if file.md5 in all_md5s:
        # compare with ya_resource_id
        # if 'resource_id' is the same, then skip, due to we have it in gsheet
        # if not, then remove from yadisk due to it is duplicate
        if all_md5s[file.md5] != file.resource_id:
            print(f"File '{file.path}' already exists in gsheet, but with different resource_id: '{file.resource_id}' with md5 '{file.md5}', removing it from yadisk")
            ya_client.remove(file.path, md5=file.md5)
            
        # todo remove `upstream_meta` check and just always return
        if not upstream_meta:
            return
    
    print(f"Processing file: '{file.path}' with md5 '{file.md5}'")

    doc = Document(
        md5=file.md5,
        mime_type=file.mime_type,
        file_name=file.name,
        ya_public_key=ya_public_key,
        ya_public_url=ya_public_url,
        ya_resource_id=file.resource_id,
        upstream_metadata_url=upstream_meta,
        full=False if "милли.китапханә/limited" in file.path else True,
    )
    
    # update gsheet
    upsert(doc)
    all_md5s[file.md5] = file.resource_id

def publish_file(client, path):
    _ = client.publish(path)
    resp = client.get_meta(path, fields = ['public_key', 'public_url'])
    return resp['public_key'], resp['public_url']


def _lookup_upstream_metadata(s3client, config):
    bucket = config["yandex"]["cloud"]["bucket"]["upstream_metadata"]
    s3client.list_objects_v2(Bucket=bucket)
    paginator = s3client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket)
    return {
         obj['Key'].removesuffix('.zip'): f"{s3client._endpoint.host}/{bucket}/{obj['Key']}"
         for page in pages
         for obj in page['Contents']
    }