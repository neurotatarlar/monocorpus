from utils import read_config
from yadisk_client import YaDisk
from rich import print
from monocorpus_models import Document
from s3 import  create_session
from monocorpus_models import Document, Session
from sqlalchemy import select
from collections import deque

BATCH_SIZE = 20

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
    'audio/mpeg',
    'text/html',
    'text/plain'
]

def sync():
    """
    Syncs files from Yandex Disk to Google Sheets.
    """
    config = read_config()
    all_md5s = get_all_md5s()
    s3client = create_session(config)
    upstream_metas = _lookup_upstream_metadata(s3client, config)

    skipped = []
    gsheets = Session()
    entry = config['yandex']['disk']['entry_point']

    with YaDisk(config['yandex']['disk']['oauth_token']) as yaclient:
        batch = []
        for file_res in _walk_yadisk(yaclient, entry):
            meta = upstream_metas.get(file_res.md5)
            doc = _process_file(
                yaclient, file_res, all_md5s,
                skipped, meta, gsheets
            )
            if doc:
                batch.append(doc)
                if len(batch) >= BATCH_SIZE:
                    gsheets.upsert(batch)
                    batch.clear()

        if batch:
            gsheets.upsert(batch)
            
    if skipped:
        print("Skipped by MIME type files:")
        print(*skipped, sep="\n")
        
def _walk_yadisk(client, root):
    """Yield all file resources under `root` on Yandex Disk."""
    queue = deque([root])
    while queue:
        current = queue.popleft()
        print(f"Visiting: '{current}'")
        for res in client.listdir(
            current,
            max_items=None,
            fields=[
                'type', 'path', 'mime_type',
                'md5', 'public_key', 'public_url',
                'resource_id', 'name'
            ]
        ):
            if res.type == 'dir':
                queue.append(res.path)
            else:
                yield res

def _process_file(ya_client, file, all_md5s, skipped_by_mime_type_files, upstream_meta, gsheets_session):

    if file.mime_type in not_document_types:
        print(f"Skipping file: '{file.path}' of type '{file.mime_type}'")
        skipped_by_mime_type_files.append((file.mime_type, file.public_url, file.path))
        return
    
    ya_public_key = file.public_key
    ya_public_url = file.public_url
    if not (ya_public_key and ya_public_url):
        ya_public_key, ya_public_url = _publish_file(ya_client, file.path)
    
    if file.md5 in all_md5s:
        # compare with ya_resource_id
        # if 'resource_id' is the same, then skip, due to we have it in gsheet
        # if not, then remove from yadisk due to it is duplicate
        if all_md5s[file.md5]['resource_id'] != file.resource_id:
            print(f"File '{file.path}' already exists in gsheet, but with different resource_id: '{file.resource_id}' with md5 '{file.md5}', removing it from yadisk")
            ya_client.remove(file.path, md5=file.md5)
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
    all_md5s[file.md5] = {"resource_id": doc.ya_resource_id, "upstream_metadata_url": doc.upstream_metadata_url} 
    return doc

def _publish_file(client, path):
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
    
def get_all_md5s():
    """
    Returns a dict of all md5s in the database with ya_resource_id
    :return: set of md5s
    """
    with Session() as s:
        res = s._get_session().execute(
            select(Document.md5, Document.ya_resource_id, Document.upstream_metadata_url)
        ).all()
        return { 
                i[0]: {"resource_id": i[1], "upstream_metadata_url": i[2]} 
                for i 
                in res 
                if i[1] is not None
        }