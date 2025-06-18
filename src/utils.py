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

prefix = "enc:"

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


def read_config(config_file: str = "config.yaml"):
    with open(get_in_workdir(file=config_file, prefix="."), 'r') as file:
        return yaml.safe_load(file)


def get_in_workdir(*dir_names: Union[str, Dirs], file: str = None, prefix: str = '~/.monocorpus'):
    dir_names = [i.value if isinstance(i, Dirs) else i for i in dir_names]
    script_parent_dir = os.path.dirname(os.path.realpath(sys.argv[0]))
    path = [script_parent_dir, '..', os.path.expanduser(prefix), *dir_names]
    path = os.path.normpath(os.path.join(*path))
    os.makedirs(path, exist_ok=True)
    if file:
        return os.path.join(path, file)
    else:
        return path

def obtain_documents(cli_params, ya_client, predicate=None, limit=None, gsheet_session = Session()):
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
    ext = f"{os.path.splitext(doc.file_name)[1]}" if doc.file_name else ""
    local_path=get_in_workdir(Dirs.ENTRY_POINT, file=f"{doc.md5}{ext}")
    if not (os.path.exists(local_path) and calculate_md5(local_path) == doc.md5):
        url = decrypt(doc.ya_public_url, config) if doc.sharing_restricted else doc.ya_public_url
        with open(local_path, "wb") as f:
            ya_client.download_public(url, f)
    return local_path

def _find(session, predicate=None, limit=None):
    statement = select(Document)
    if predicate is not None:
        statement = statement.where(predicate)
    if limit:
        statement = statement.limit(limit)
    
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
        for res in client.listdir(
            current,
            max_items=None,
            fields=fields
        ):
            if res.type == 'dir':
                queue.append(res.path)
            else:
                yield res
                
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