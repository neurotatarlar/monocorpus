from monocorpus_models import Document, Session
from sqlalchemy import select
from utils import read_config, encrypt, get_in_workdir, decrypt
from yadisk_client import YaDisk
from rich import print
from s3 import create_session
from rich.progress import track
from yadisk.exceptions import PathNotFoundError
import os
from dirs import Dirs

def restore():
    config = read_config()
    s3client = create_session(config)
    documents_bucket = config["yandex"]["cloud"]["bucket"]["document"]

    with Session() as session, YaDisk(config['yandex']['disk']['oauth_token']) as ya_client:
        res = session.query(select(Document).where(Document.sharing_restricted.is_(True)))
    with Session() as session:
        for doc in track(res):
            try:
                if not doc.ya_public_url:
                    _restore(doc, s3client, documents_bucket, ya_client, session, config)
                    continue
                try:
                    pub_url = decrypt(doc.ya_public_url, config) if doc.sharing_restricted else doc.ya_public_url
                    _ = ya_client.get_public_meta(pub_url, fields=["type"])
                except PathNotFoundError:
                    _restore(doc, s3client, documents_bucket, ya_client, session, config)
            except Exception as e:
                import traceback
                print(f"[red]Error during processing document: {e}: {traceback.format_exc()}[/red]")
                
                
def _restore(doc, s3client, documents_bucket, ya_client, session, config):
    # print(f"File not found in Yandex Disk by public url `{doc.md5}`")
    if (meta := get_meta(doc.ya_path, ya_client)) and meta.md5 == doc.md5:
        # here if file exists and it is the same as in ghseets
        # no need to upload it again
        remote_path = doc.ya_path
        print("File still exists in yandex disk")
    else:
        ext = None
        if doc.ya_path:
            _, ext = os.path.splitext(doc.ya_path)
        if not ext:
            # If the file has no extension, we try to guess it by mime type
            # or use a default extension if mime type is unknown
            ext = _extension_by_mime_type(doc.mime_type)
        
        file = f"{doc.md5}{ext}"
        local_path=get_in_workdir(Dirs.ENTRY_POINT, file=file)
        if not os.path.exists(local_path):
            # download from s3
            print(f"Downloading file from s3: `{file}`")
            if doc.document_url:
                file = doc.document_url.removeprefix('https://storage.yandexcloud.net/ttdoc/')
            s3client.download_file(documents_bucket, file, local_path)
        else:
            print(f"Found file `{file}` locally")
        upload_res = ya_client.upload(local_path, doc.ya_path, overwrite=True)
        remote_path = upload_res.path
    ya_public_key, ya_public_url, ya_path, ya_resource_id = _publish_file(ya_client, remote_path)
    doc.ya_public_key = ya_public_key
    doc.ya_public_url=encrypt(ya_public_url, config) if doc.sharing_restricted else ya_public_url
    doc.ya_path = ya_path.removeprefix('disk:') 
    doc.ya_resource_id = ya_resource_id
    session.update(doc)
    print(f"Restored file `{doc.md5}`")
    
def get_meta(path, ya_client):
    try:
        if not path:
            return None
        return ya_client.get_meta(path, fields=["md5"])
    except PathNotFoundError:
        return None    
    
    
def _extension_by_mime_type(mime_type):
    if mime_type == 'application/vnd.openxmlformats-officedocument.wordprocessingml.document':
        return '.docx'
    elif mime_type == 'text/plain':
        return '.txt'
    elif mime_type == 'text/html':
        return '.html'
    elif mime_type == 'application/pdf':
        return '.pdf'
    elif mime_type == 'image/vnd.djvu':
        return '.djvu'
    else:
        raise ValueError(f"Unexpected mime type '{mime_type}'")
    
def _publish_file(client, path):
    try:
        _ = client.unpublish(path)
    except: 
        pass
    _ = client.publish(path)
    resp = client.get_meta(path, fields = ['public_key', 'public_url', 'path', 'resource_id'])
    return resp['public_key'], resp['public_url'], resp.path, resp.resource_id
