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

# todo extract metadata from docs
# todo add books from other folders
# todo fix bbc
# todo upload external metadata
# separate full and partial documents
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

def flatten():
    # visit dir
    # if file, process as usual
    # if dir, then check content
    # special treatment for 'limited' folder
    config = read_config()
    s3client = create_session(config)
    dir_to_visit = '/НейроТатарлар/kitaplar/_все книги/милли.китапханә/limited'
    with YaDisk(config['yandex']['disk']['oauth_token']) as client:
        listing = client.listdir(dir_to_visit, max_items=None, fields=['type', 'path'])
        for dir in track([res for res in listing if res.type == 'dir'], "Flatteninfg folders"):
            print(f"Visiting: '{dir.path}'")
            dir_content = [c for c in client.listdir(dir.path, fields=['name', 'path', 'type', 'md5']) if c.type == 'file']
            metas = [m for m in dir_content if m.name == 'metadata.json']
            docs = [d for d in dir_content if d.name.endswith(".pdf")]

            doc = None 
            meta = None
            if docs and len(docs) == 1:
                doc = docs[0]
            
            if metas and len(metas) == 1 and doc:
                print("About to upload meta to S3: " + dir.path)
                meta = metas[0]
                tmp_file = get_in_workdir(file=".tmp")
                client.download(meta.path, tmp_file)
                tmp_zip_file = tmp_file + ".zip"
                with zipfile.ZipFile(tmp_zip_file, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zf, open(tmp_file, "r") as meta_file:
                    meta_raw = json.load(meta_file)
                    zf.writestr("metadata.json", json.dumps(meta_raw, ensure_ascii=False, indent=None, separators=(',', ':')))
                bucket = config["yandex"]["cloud"]["bucket"]["upstream_metadata"]
                upload_file(path=tmp_zip_file, bucket=bucket, key=f"{doc.md5}.zip", session=s3client, skip_if_exists=False)
                print("Uploaded meta to S3 " + dir.path)
            
            if doc:
                dst_path = os.path.join(os.path.dirname(os.path.dirname(doc.path)), doc.name)
                print(f"Moving from '{doc.path}' to '{dst_path}'")
                client.move(doc.path, dst_path, overwrite=True)
                
            print("Removing ", dir.path)
            client.remove(dir.path)
            
    exit(0)

def sync():
    flatten()
    
    """
    Syncs files from Yandex Disk to Google Sheets.
    """
    all_md5s = get_all_md5s()
    config = read_config()
    dirs_to_visit = [config['yandex']['disk']['entry_point']]
    skipped_by_mime_type_files = []
    s3client = create_session(config)
    with YaDisk(config['yandex']['disk']['oauth_token']) as client:
        while dirs_to_visit:
            current_dir = dirs_to_visit.pop(0)
            if current_dir.startswith("/НейроТатарлар/kitaplar/_все книги/милли.китапханә/") or current_dir.startswith("disk:/НейроТатарлар/kitaplar/_все книги/милли.китапханә/"):
                continue
            print(f"Visiting: '{current_dir}'")
            listing = client.listdir(current_dir, max_items=None, fields=['type', 'path', 'mime_type', 'md5', 'public_key', 'public_url', 'resource_id', 'name'])
            for resource in listing:
                match _type := resource['type']:
                    case 'dir':
                        dirs_to_visit.append(resource['path'])
                    case 'file':
                        _process_file(client, resource, all_md5s, skipped_by_mime_type_files, s3client, config)
                    case _:
                        print(f"Unknown type: '{_type}'")
    
    print("Skipped by MIME type files:")
    for s in skipped_by_mime_type_files:
        print(s)


def _process_file(ya_client, file, all_md5s, skipped_by_mime_type_files, s3client, config):

    if file.name == 'metadata.json':
        # print("removing file ", file.path)
        # tmp_file = get_in_workdir(file=".tmp")
        # ya_client.download(file.path, tmp_file)
        # bucket = config["yandex"]["cloud"]["bucket"]["upstream_metadata"]
        # upload_file(path=tmp_file, bucket=bucket, key=file.md5, session=s3client, skip_if_exists=True)
        # ya_client.remove(file.path, md5=file.md5)
        return
    
    if file.name == 'parta_decrypted.zip':
        # print("removing file ", file.path)
        # ya_client.remove(file.path)
        return
    
    if file.mime_type in not_document_types:
        print(f"Skipping file: '{file.path}' of type '{file.mime_type}'")
        skipped_by_mime_type_files.append((file.mime_type, file.public_url, file.path))
        return
    
    if file.md5 in all_md5s:
        # compare with ya_resource_id
        # if 'resource_id' is the same, then skip, due to we have it in gsheet
        # if not, then remove from yadisk due to it is duplicate
        if all_md5s[file.md5] != file.resource_id:
            print(f"File '{file.path}' already exists in gsheet, but with different resource_id: '{file.resource_id}' with md5 '{file.md5}', removing it from yadisk")
            ya_client.remove(file.path, md5=file.md5)
        return
    
    print(f"Processing file: '{file.path}' with md5 '{file.md5}'")

    ya_public_key = file.public_key
    ya_public_url = file.public_url
    if not (ya_public_key and ya_public_url):
        file.public_key , ya_public_url = publish_file(ya_client, file.path)
    doc = Document(
        md5=file.md5,
        mime_type=file.mime_type,
        file_name=file.name,
        ya_public_key=ya_public_key,
        ya_public_url=ya_public_url,
        ya_resource_id=file.resource_id,
        full=True,
    )
    
    # # todo remove me later
    # dst_path = os.path.join(os.path.dirname(os.path.dirname(file.path)), file.name)
    # print(f"Moving from '{file.path}' to '{dst_path}'")
    # ya_client.move(file.path, dst_path)
    
    # update gsheet
    upsert(doc)
    all_md5s[file.md5] = file.resource_id

def publish_file(client, path):
    _ = client.publish(path)
    resp = client.get_meta(path, fields = ['public_key', 'public_url'])
    return resp['public_key'], resp['public_url']
