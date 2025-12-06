"""
Public Links Verification and Restoration Module

This module verifies and restores public sharing links for documents stored in Yandex.Disk.
It handles both regular and sharing-restricted documents, managing their encryption states
and maintaining consistency between S3 storage and Yandex.Disk.

Key Features:
1. Link Verification
   - Validates existing public URLs
   - Handles encrypted URLs for restricted documents
   - Checks document integrity via MD5 hashes

2. Document Restoration
   - Downloads documents from S3 when needed
   - Re-uploads to Yandex.Disk if missing
   - Recreates public sharing links
   - Updates database records with new metadata

3. Security Management
   - Encrypts URLs for sharing-restricted documents
   - Maintains proper access control during restoration
   - Handles document republishing securely

Functions:
    check(): Main verification and restoration process
    _restore(): Handles document restoration workflow
    get_meta(): Retrieves Yandex.Disk metadata for a path
    _extension_by_mime_type(): Maps MIME types to file extensions
    _publish_file(): Manages document publishing process

Process Flow:
1. Query documents from database
2. For each document:
   - Verify public URL accessibility
   - If URL is invalid or missing:
     a. Check if file exists in Yandex.Disk
     b. Download from S3 if needed
     c. Upload to Yandex.Disk
     d. Create new public link
     e. Update database record

Requirements:
- Yandex.Disk OAuth token
- S3 credentials and bucket configuration
- Database access
- Local storage for temporary files

Error Handling:
- Graceful handling of missing files
- Recovery from failed uploads
- Proper encryption state management
- Transaction safety for database updates

Usage:
    The module is typically run as a maintenance task to ensure
    all documents remain accessible through their public links.
"""

from sqlalchemy import select
from utils import read_config, encrypt, get_in_workdir, decrypt, get_session
from yadisk_client import YaDisk
from rich import print
from s3 import create_session
from rich.progress import track
from yadisk.exceptions import PathNotFoundError
import os
from dirs import Dirs
from models import Document

def check():
    config = read_config()
    s3client = create_session(config)
    documents_bucket = config["yandex"]["cloud"]["bucket"]["document"]

    with get_session() as session:
        docs = list(session.scalars(select(Document)))
    with YaDisk(config['yandex']['disk']['oauth_token'], proxy=config['proxy']) as ya_client:
        for doc in track(docs, description="Checking public links"):
            try:
                if not doc.ya_public_url:
                    _restore(doc, s3client, documents_bucket, ya_client, config)
                    continue
                try:
                    pub_url = decrypt(doc.ya_public_url, config) if doc.sharing_restricted else doc.ya_public_url
                    _ = ya_client.get_public_meta(pub_url, fields=["type"])
                except PathNotFoundError:
                    _restore(doc, s3client, documents_bucket, ya_client, config)
            except Exception as e:
                import traceback
                print(f"[red]Error during processing document: {e}: {traceback.format_exc()}[/red]")
                
                
def _restore(doc, s3client, documents_bucket, ya_client, config):
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
    
    with get_session() as session:
        session.merge(doc)
        session.commit()
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
