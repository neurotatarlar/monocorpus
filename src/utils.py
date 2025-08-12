import os
from dirs import Dirs
import sys
import yaml
from typing import Union
import hashlib
from monocorpus_models import Document, Session
from sqlalchemy import select
from collections import deque
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
import base64
from datetime import datetime, timezone, timedelta
import json

prefix = "enc:"

workdir = "~/.monocorpus"


def read_config(config_file: str = "config.yaml"):
    with open(get_in_workdir(file=config_file, prefix="."), 'r') as file:
        return yaml.safe_load(file)

def pick_files(dir_path: Union[str, Dirs]):
    return [
        os.path.normpath(os.path.join(dir_name, f))
        for dir_name, _, files
        in os.walk(get_in_workdir(dir_path))
        for f
        in files
    ]

def calculate_md5(file_path: str):
    """
    Calculates MD5 hash of the file

    :param file_path: path to the file
    :return: MD5 hash of the file
    """
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(2048), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def get_in_workdir(*dir_names: Union[str, Dirs], file: str = None, prefix: str = workdir):
    dir_names = [i.value if isinstance(i, Dirs) else i for i in dir_names]
    script_parent_dir = os.path.dirname(os.path.realpath(sys.argv[0]))
    path = [script_parent_dir, '..', os.path.expanduser(prefix), *dir_names]
    path = os.path.normpath(os.path.join(*path))
    os.makedirs(path, exist_ok=True)
    if file:
        return os.path.join(path, file)
    else:
        return path

def obtain_documents(cli_params, ya_client, predicate=None, limit=None, offset=None, gsheet_session = Session()):
    def _yield_by_md5(_md5, _predicate):
        print(f"Looking for document by md5 '{_md5}'")
        if _predicate is None:
            _predicate = Document.md5.is_(_md5)
        else:
            _predicate &= Document.md5.is_(_md5)
        yield from _find(gsheet_session, predicate=_predicate, limit=1)

    def _yield_by_path(_path, _predicate):
        _meta = ya_client.get_meta(_path, fields=['md5', 'type', 'path'])
        if _meta.type == 'file':
            yield from _yield_by_md5(_meta.md5, _predicate)
        elif _meta.type == 'dir':
            print(f"Traversing documents by path '{_path}'")
            unprocessed_docs = {d.md5: d for d in _find(gsheet_session, _predicate)}
            counter = 0
            dirs_to_visit = [_meta.path]
            while dirs_to_visit:
                dir = dirs_to_visit.pop(0)
                for item in ya_client.listdir(dir, max_items=None, fields=['md5', 'type', 'path']):
                    if item.type == 'dir':
                        dirs_to_visit.append(item.path)
                    elif item.type == 'file' and (doc := unprocessed_docs.get(item.md5)):
                        yield doc
                        if limit:
                            counter += 1
                            if counter >= limit:
                                return
                        
    if cli_params.md5:
        yield from _yield_by_md5(cli_params.md5, predicate)
    elif cli_params.path:
        yield from _yield_by_path(cli_params.path, predicate)
    else:
        print("Traversing all unprocessed documents")
        yield from _find(gsheet_session, predicate=predicate, limit=limit)

def download_file_locally(ya_client, doc, config):
    def _extension_by_mime_type(mime_type):
        if mime_type == 'application/vnd.openxmlformats-officedocument.wordprocessingml.document':
            return '.docx'
        elif mime_type == 'text/plain':
            return '.txt'
        elif mime_type == 'text/html':
            return '.html'
        elif mime_type == 'application/pdf':
            return '.pdf'
        else:
            raise ValueError("Unexpected mime type")
        
    _, ext = os.path.splitext(doc.file_name)
    if not ext:
        # If the file has no extension, we try to guess it by mime type
        # or use a default extension if mime type is unknown
        ext = _extension_by_mime_type(doc.mime_type)
    local_path=get_in_workdir(Dirs.ENTRY_POINT, file=f"{doc.md5}{ext}")
    if not (os.path.exists(local_path) and calculate_md5(local_path) == doc.md5):
        url = decrypt(doc.ya_public_url, config) if doc.sharing_restricted else doc.ya_public_url
        with open(local_path, "wb") as f:
            ya_client.download_public(url, f)
    return local_path

def _find(session, predicate=None, limit=None, offset=None):
    statement = select(Document)
    if predicate is not None:
        statement = statement.where(predicate)
    if limit:
        statement = statement.limit(limit)
        
    if offset:
        statement.offset(offset)
    
    yield from session.query(statement)
    
    
def walk_yadisk(client, root, fields = [
                'type', 'path', 'mime_type',
                'md5', 'public_key', 'public_url',
                'resource_id', 'name'
    ]):
    """Yield all file resources under `root` on Yandex Disk."""
    fields.append('type')
    queue = deque([root])
    while queue:
        current = queue.popleft()
        print(f"Visiting '{current}'")
        empty = True
        for res in client.listdir(
            current,
            max_items=None,
            fields=fields
        ):
            empty = False
            if res.type == 'dir':
                queue.append(res.path)
            else:
                yield res
        if empty:
            print(f"Removing folder `{current}` because it is empty")
            client.remove(current, force_async=True, wait=False)
                
def encrypt(url, config):
    key = base64.urlsafe_b64decode(config["encryption_key"])
    aesgcm = AESGCM(key)
    nonce = os.urandom(12)
    encrypted = aesgcm.encrypt(nonce, url.encode(), None)
    chiphercode = base64.urlsafe_b64encode(nonce + encrypted).decode()
    return f"{prefix}{chiphercode}"

def decrypt(ciphertext, config):
    encrypted_url = ciphertext.removeprefix(prefix)
    data = base64.urlsafe_b64decode(encrypted_url)
    nonce, ct = data[:12], data[12:]
    key = base64.urlsafe_b64decode(config["encryption_key"])
    aesgcm = AESGCM(key)
    return aesgcm.decrypt(nonce, ct, None).decode()

def load_expired_keys(dir = 'expired_keys'):
    os.makedirs(dir, exist_ok=True)
    ekf = os.path.join(dir, f"expired_keys_{_get_bucket_id()}.json")
    if os.path.exists(ekf):
        with open(ekf, "r") as f:
            return set(json.load(f))
    else: return set()
    

def dump_expired_keys(keys, dir = 'expired_keys'):
    os.makedirs(dir, exist_ok=True)
    ekf = os.path.join(dir, f"expired_keys_{_get_bucket_id()}.json")
    with open(ekf, "w") as f:
        json.dump(list(keys), f, ensure_ascii=False, indent=4)
        

def _get_bucket_id():
    """Return bucket like '20250810_1' or '20250811_0' based on 09:00 UTC cutoff."""
    now = datetime.now(timezone.utc)

    # If before 09:00 UTC, we are still in the *previous day's* second bucket
    if now.hour < 9:
        date = (now - timedelta(days=1)).strftime("%Y%m%d")
        bucket_num = 1
    else:
        date = now.strftime("%Y%m%d")
        bucket_num = 0

    return f"{date}_{bucket_num}"