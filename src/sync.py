from utils import read_config
from gsheets import get_all_md5s, upsert
from yadisk_client import YaDisk
from rich import print
from monocorpus_models import Document

# todo remove all metadata.json files from gsheet and unpublish them
# todo extract metadata from docs
# todo add books from other folders
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
]

def sync():
    """
    Syncs files from Yandex Disk to Google Sheets.
    """
    all_md5s = get_all_md5s()
    config = read_config()
    dirs_to_visit = [config['yandex']['disk']['entry_point']]
    skipped_by_mime_type_files = []
    with YaDisk(config['yandex']['disk']['oauth_token']) as client:
        while dirs_to_visit:
            current_dir = dirs_to_visit.pop(0)
            print(f"Visiting: '{current_dir}'")
            for resource in client.listdir(current_dir, max_items=None, fields=['type', 'path', 'mime_type', 'md5', 'public_key', 'public_url', 'resource_id', 'name']):
                match _type := resource['type']:
                    case 'dir':
                        dirs_to_visit.append(resource['path'])
                    case 'file':
                        _process_file(client, resource, all_md5s, skipped_by_mime_type_files)
                    case _:
                        print(f"Unknown type: '{_type}'")
    
    print("Skipped by MIME type files:")
    for s in skipped_by_mime_type_files:
        print(s)


def _process_file(ya_client, file, all_md5s, skipped_by_mime_type_files):
    path = file["path"]
    if path.endswith('metadata.json') or path.endswith('parta_decrypted.zip'):
        return
    
    if (mime_type := file['mime_type']) in not_document_types:
        print(f"Skipping file: '{file['path']}' of type '{file['mime_type']}'")
        skipped_by_mime_type_files.append((file['mime_type'], file["public_url"], file['path']))
        return
    
    if (md5 := file["md5"]) in all_md5s:
        # compare with ya_resource_id
        # if 'resource_id' is the same, then skip, due to we have it in gsheet
        # if not, then remove from yadisk due to it is duplicate
        if all_md5s[md5] != file["resource_id"]:
            print(f"File '{path}' already exists in gsheet, but with different resource_id: '{file['resource_id']}' with md5 '{md5}', removing it from yadisk")
            ya_client.remove(path, md5=md5, )
        return
    
    print(f"Processing file: '{file['path']}' with md5 '{file['md5']}'")
    ya_public_key = file["public_key"]
    ya_public_url = file["public_url"]
    if not (ya_public_key and ya_public_url):
        ya_public_key, ya_public_url = publish_file(ya_client, path)
    doc = Document(
        md5=md5,
        mime_type=mime_type,
        file_name=file["name"],
        ya_public_key=ya_public_key,
        ya_public_url=ya_public_url,
        ya_resource_id=file["resource_id"],
    )
    
    # update gsheet
    upsert(doc)
    all_md5s[md5] = file["resource_id"]

def publish_file(client, path):
    _ = client.publish(path)
    resp = client.get_meta(path, fields = ['public_key', 'public_url'])
    return resp['public_key'], resp['public_url']
