from utils import read_config, walk_yadisk
from yadisk_client import YaDisk
from rich import print
from monocorpus_models import Document
from s3 import  create_session
from monocorpus_models import Document, Session
from sqlalchemy import select
from sweep import move_to_filtered_out, not_document_types

BATCH_SIZE = 20

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
    entry_point = config['yandex']['disk']['entry_point']
    with YaDisk(config['yandex']['disk']['oauth_token']) as yaclient:
        batch = []
        for file_res in walk_yadisk(yaclient, entry_point):
            meta = upstream_metas.get(file_res.md5)
            doc = _process_file(
                yaclient, file_res, all_md5s,
                skipped, meta, config
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
        
def _process_file(ya_client, file, all_md5s, skipped_by_mime_type_files, upstream_meta, config):

    _should_be_skipped, mime_type = should_be_skipped(file)
    if _should_be_skipped:
        print(f"Moving file '{file.path}' from target folder because of mime_type type '{file.mime_type}'")
        move_to_filtered_out(file, config, ya_client, 'nontextual')
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
        mime_type=mime_type,
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
        
def should_be_skipped(file):
    if file.mime_type in not_document_types:
        # sometimes valid PDF docs detected as octet-stream
        if file.mime_type == 'application/octet-stream' and file.path.endswith(".pdf"):
            return False, 'application/pdf'
        elif file.mime_type == 'text/html' and file.path.endswith(".txt"):
            return False, 'text/plain'
        elif file.mime_type == 'text/html' and file.path.endswith(".doc"):
            return False, 'text/plain'
        else:
            return True, file.mime_type 
    return False, file.mime_type 