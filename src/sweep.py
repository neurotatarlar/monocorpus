import os
from monocorpus_models import Document, Session
from utils import read_config, walk_yadisk
from sqlalchemy import select
from yadisk_client import YaDisk
from s3 import  create_session
from rich import print
from sqlalchemy import delete

tatar_bcp_47_codes = ['tt-Latn-x-zamanalif', 'tt-Cyrl', 'tt-Latn-x-yanalif', 'tt-Arab', 'tt-Latn']

not_document_types = [
    'application/vnd.android.package-archive',
    'image/jpeg',
    'application/x-zip-compressed',
    'application/zip'
    'application/octet',
    'application/octet-stream',
    'text/x-python'
    'application/x-gzip',
    'application/x-rar',
    'application/x-download',
    "application/json",
    'audio/mpeg',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    'text/javascript',
    'application/javascript',
    'application/x-shockwave-flash',
    'text/css',
    'application/x-javascript',
    'application/x-shockwave-flash'
]

def sweep():
    config = read_config()
    s3client = create_session(config)
    with Session() as session, YaDisk(config['yandex']['disk']['oauth_token']) as yaclient:
        print("Querying non tatar documents")
        nontatar_docs = Session().query(select(Document).where(Document.language.not_in(tatar_bcp_47_codes)))
        nontatar_docs = {d.md5: d for d in nontatar_docs}
        print(f"Found {len(nontatar_docs)} nontatar docs")
        print("Querying non textual docs")
        nontextual_docs = Session().query(select(Document).where(Document.mime_type.in_(not_document_types)))
        nontextual_docs = {d.md5: d for d in nontextual_docs}
        print(f"Found {len(nontextual_docs)} nontextual docs")

        docs_for_wiping = {}
        docs_for_wiping.update(nontatar_docs)
        docs_for_wiping.update(nontextual_docs)
        if not docs_for_wiping:
            print("No docs for wiping found")
            return
        
        print("Removing objects from s3 storage")
        _remove_from_s3(docs_for_wiping.keys(), s3client, config)
        
        print("Moving and unpublishing files")
        entry_point = config['yandex']['disk']['entry_point']
        for file in walk_yadisk(client=yaclient, root=entry_point, fields=["path", "md5"]):
            if d := docs_for_wiping.get(file.md5, None):
                print(f"Moving file '{file.path}'")
                if d.language not in tatar_bcp_47_codes:
                    move_to_filtered_out(file, config, yaclient, f"nontatar/{d.language}")
                else:
                    move_to_filtered_out(file, config, yaclient, "nontextual")
                
        print("Removing from google sheets")
        for md5 in docs_for_wiping:
            session._get_session().execute(delete(Document).where(Document.md5.is_(md5)))
            
def move_to_filtered_out(file, config, ya_client, parent_dir):
    # For each file
    # 1. move file to dedicated folder
    # 2. unpublish file if it has public link
    filtered_out_dir = config['yandex']['disk']['filtered_out']
    entry_point = config['yandex']['disk']['entry_point']
    
    old_path = file.path.removeprefix('disk:')
    _rel_path = os.path.relpath(old_path, entry_point)
    new_path = os.path.join(filtered_out_dir, parent_dir, _rel_path)
    if old_path != new_path:
        ya_client.create_folders(os.path.dirname(new_path))
        ya_client.move(file.path, new_path, n_retries=5, retry_interval=30)
    ya_client.unpublish(new_path)
    
def _remove_from_s3(md5s, s3client, config):
    if not md5s:
        return
    content_bucket = config["yandex"]["cloud"]['bucket']['content']
    content_chunks_bucket = config["yandex"]["cloud"]['bucket']['content_chunks']
    documents_bucket = config["yandex"]["cloud"]['bucket']['document']
    images_bucket = config["yandex"]["cloud"]['bucket']['image']
    upstream_metadatas_bucket = config["yandex"]["cloud"]['bucket']['upstream_metadata']
    metadatas_bucket = config["yandex"]["cloud"]['bucket']['metadata']
    buckets = [content_bucket, content_chunks_bucket, documents_bucket, images_bucket, upstream_metadatas_bucket, metadatas_bucket]
    for bucket in buckets:
        paginator = s3client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket)

        keys_to_remove = []
        for page in pages:
            for obj in page.get('Contents', []):
                key = obj['Key']
                if any(key.startswith(md5) for md5 in md5s):
                    keys_to_remove.append({'Key': key})
                    
        if keys_to_remove:
            print(f"Removing {len(keys_to_remove)} objects from bucket '{bucket}'")
            for i in range(0, len(keys_to_remove), 1000):
                batch = keys_to_remove[i:i+1000]
                s3client.delete_objects(Bucket=bucket, Delete={'Objects': batch, 'Quiet': True})
