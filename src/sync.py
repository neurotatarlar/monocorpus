from utils import read_config, walk_yadisk, encrypt, get_in_workdir, download_file_locally
from yadisk_client import YaDisk
from rich import print
from monocorpus_models import Document
from s3 import  create_session
from monocorpus_models import Document, Session
from sqlalchemy import text, select, delete
import json
from dirs import Dirs
import os
from collections import defaultdict
import pymupdf
import typer
from rich import print

tatar_bcp_47_codes = ['tt-Latn-x-zamanalif', 'tt-Cyrl', 'tt-Latn-x-yanalif', 'tt-Arab', 'tt-Latn']
not_document_types = [
    'application/vnd.android.package-archive',
    'image/jpeg',
    'application/x-zip-compressed',
    'application/zip'
    'application/octet',
    'text/x-python',
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
    'application/x-shockwave-flash',
    'image/tiff',
    'text/x-python-script',
    'audio/mp3',
    'audio/x-wav',
    'image/gif',
    'audio/mp3',
    'audio/midi',
    'image/vnd.adobe.photoshop',
    'video/3gpp',
    'application/x-7z-compressed'
]

def sync():
    """
    Syncs files from Yandex Disk to Google Sheets.
    """
    config = read_config()
    s3client = create_session(config)

    with Session() as session, YaDisk(config['yandex']['disk']['oauth_token']) as yaclient: 
        session.query(text("select 1"))
        print("Requesting all upstream metadata urls") 
        upstream_metas = _lookup_upstream_metadata(s3client, config)
        print("Requesting all md5s") 
        all_md5s = get_all_md5s()
        print("Defining docs for wiping") 
        docs_for_wiping = _define_docs_for_wiping(yaclient, config) 
        
        if docs_for_wiping:
            print("Removing objects from s3 storage")
            _remove_from_s3(docs_for_wiping.keys(), s3client, config)
        else:
            print("No docs for wiping found")
            
        print("Syncing yadisk with Google sheets")
        entry_point = config['yandex']['disk']['entry_point']
        skipped = []
        for file in walk_yadisk(client=yaclient, root=entry_point):
            try:
                if dir_to_move := docs_for_wiping.get(file.md5, None):
                    # the file marked for wiping
                    _move_to_filtered_out(file, config, yaclient, dir_to_move)
                    session._get_session().execute(delete(Document).where(Document.md5.is_(file.md5)))
                    del docs_for_wiping[file.md5]
                    flush(docs_for_wiping)
                else:
                    meta = upstream_metas.get(file.md5)
                    if doc := _process_file(
                        yaclient, file, all_md5s,
                        skipped, meta, config
                    ):
                        session.upsert([doc])
            except Exception as e:
                import traceback
                print(f"[red]Error during syncing: {type(e).__name__}: {e} {traceback.format_exc()}[/red]")
        if skipped:
            print("Skipped by MIME type files:")
            print(*skipped, sep="\n")
            
def _move_to_filtered_out(file, config, ya_client, parent_dir):
    # For each file
    # 1. move file to dedicated folder
    # 2. unpublish file if it has public link
    filtered_out_dir = config['yandex']['disk']['filtered_out']
    entry_point = config['yandex']['disk']['entry_point']
    
    if parent_dir == 'void':
        print(f"[magenta]Removing file '{file.md5}'('{file.path}')[/magenta]")
        ya_client.remove(file.path, n_retries=5, retry_interval=30)
    else:
        old_path = file.path.removeprefix('disk:')
        _rel_path = os.path.relpath(old_path, entry_point)
        new_path = os.path.join(filtered_out_dir, parent_dir, _rel_path)
        print(f"[cyan]Moving file '{file.md5}' from '{old_path} to '{new_path}'[/cyan]")
        ya_client.create_folders(os.path.dirname(new_path))
        ya_client.move(file.path, new_path, n_retries=5, retry_interval=30, overwrite=True)
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
        

def _define_docs_for_wiping(yaclient, config):
    docs_for_wiping = _get_wiping_plan()

    # print("Querying non tatar documents")
    # non_tatar_docs = Session().query(select(Document).where(Document.language.not_in(tatar_bcp_47_codes)))
    # non_tatar_docs = {d.md5: f"nontatar/{'-'.join(sorted(d.language.split(', ')))}" for d in non_tatar_docs}
    # print(f"Found {len(non_tatar_docs)} nontatar docs")
    # docs_for_wiping.update(non_tatar_docs)
    # flush(docs_for_wiping)
    
    # print("Querying non textual docs")
    # nontextual_docs = Session().query(select(Document).where(Document.mime_type.in_(not_document_types)))
    # nontextual_docs = {d.md5: "nontextual" for d in nontextual_docs}
    # print(f"Found {len(nontextual_docs)} nontextual docs")
    # docs_for_wiping.update(nontextual_docs)
    # flush(docs_for_wiping)
    
    _dedup_by_isbn(docs_for_wiping, yaclient, config)
    
    return docs_for_wiping
    
def _dedup_by_isbn(plan, yaclient, config):
    print("Deduplicating by ISBN")
    # Get all docs that have ISBNs
    md5s_to_docs = { doc.md5 : doc for doc in  Session().query(select(Document).where(Document.isbn.is_not(None)))}
    
    # Group them by ISBN
    isbns_to_docs = defaultdict(set)
    for doc in md5s_to_docs.values():
        isbns = doc.isbn.strip().split(',')
        isbns = ", ".join(sorted([isbn.strip() for isbn in isbns if isbn.strip()]))
        isbns_to_docs[isbns].add(doc.md5)
            
    # Find duplicates
    duplicated_isbn_to_md5s = defaultdict(set)
    duplicated_docs_md5s = set()
    for isbn, md5s in isbns_to_docs.items():
        if len(md5s) > 1 and isbn:
            print(f"Found duplicate ISBN: '{isbn}' with md5s {md5s}")
            duplicated_isbn_to_md5s[isbn].update(md5s)
            duplicated_docs_md5s.update(md5s)
    del isbns_to_docs
        
    if not duplicated_isbn_to_md5s:
        print("No duplicate ISBNs found, exiting...")
        return
    
    print(f"Downloading books with {len(duplicated_docs_md5s)} duplicate ISBNs")
    md5_to_local_path = {
        doc_md5: download_file_locally(yaclient, md5s_to_docs[doc_md5], config)
        for doc_md5
        in duplicated_docs_md5s
    }
    del duplicated_docs_md5s
        
    for isbn, md5s in duplicated_isbn_to_md5s.items():
        
        def _define_docs_to_move(_docs):
            _full_docs = set([d for d in _docs if d.full == True])
            # if we have only one full document among duplicates then keep it and move anothers
            if len(_full_docs) == 1:
                return _docs - _full_docs
            _pdf_docs = set([d for d in _docs if d.mime_type in ['application/pdf', 'application/x-pdf'] and d.full == True])
            # if we have exactly one full pdf among duplicates then keep it and move anothers
            if len(_pdf_docs) == 1:
                return _docs - _pdf_docs
            _extracted_pdf_docs = set([d for d in _pdf_docs if d.content_url])
            #  if we have multiple pdf docs, but only one of them already extracted then keep it and move anothers
            if len(_extracted_pdf_docs) == 1:
                return _docs - _extracted_pdf_docs
            
            _choices = {idx: doc for idx, doc in enumerate(sorted(_docs, key=lambda d: d.ya_public_url), start=1)}
            _hint = []
            _params = set()
            for idx, doc in _choices.items():
                local_path = md5_to_local_path[doc.md5]
                if doc.mime_type in ['application/pdf', 'application/x-pdf']:
                    with pymupdf.open(local_path) as pdf_doc:
                        pages_count = str(pdf_doc.page_count)
                else:
                    pages_count = "N/A"
                size = round(os.path.getsize(local_path) / 1024 / 1024, 2)
                _hint.append(f"{idx}: {doc.md5} '{local_path}' {size} {pages_count} {doc.full} {doc.mime_type} {f' {doc.content_url}' if doc.content_url else ''}")
                _params.add(f"{pages_count}-{size}-{doc.mime_type.strip()}-{doc.full}")
            if len(_params) == 1:
                # all files have same size and pages count, just pick the first
                return _docs - {_choices[1]}
            else:
                # ask user to choose which document to keep
                _hint = "\n".join(_hint)
                res = typer.prompt(f"Multiple documents with ISBN '{isbn}' found, choose which one to keep:\n{_hint}\n", prompt_suffix="> ")
                if res.isdigit() and int(res) in _choices:
                    return _docs - {_choices[int(res)]}
                else:
                    print(f"Invalid choice '{res}', skipping ISBN {isbn}")
                    return None

        docs_same_isbn = {md5s_to_docs[md5] for md5 in md5s}
        docs_for_wiping = _define_docs_to_move(docs_same_isbn)
        if docs_for_wiping:
            plan.update({d.md5: f"duplicated_isbn/{isbn}" for d in docs_for_wiping})
            flush(plan)

    
def _get_wiping_plan():
    marked_for_wiping = get_in_workdir(Dirs.WIPING_PLAN, file="marked_for_wiping.json")
    if not os.path.exists(marked_for_wiping):
        print("No marked for wiping file found, creating a new one")
        with open(marked_for_wiping, 'w') as f:
            json.dump({}, f)
    with open(marked_for_wiping, 'r') as f:
        return json.load(f)
    
def flush(plan):
    marked_for_wiping = get_in_workdir(Dirs.WIPING_PLAN, file="marked_for_wiping.json")
    with open(marked_for_wiping, 'w') as f:
        json.dump(plan, f, indent=4, ensure_ascii=False)

        
def _process_file(ya_client, file, all_md5s, skipped_by_mime_type_files, upstream_meta, config):

    _should_be_skipped, mime_type = should_be_skipped(file)
    if _should_be_skipped:
        _move_to_filtered_out(file, config, ya_client, 'nontextual')
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
    
    print(f"[green]Adding file to gsheets '{file.path}' with md5 '{file.md5}'[/green]")

    sharing_restricted = config["yandex"]["disk"]["hidden"] in file.path 
    doc = Document(
        md5=file.md5,
        mime_type=mime_type,
        file_name=file.name,
        ya_public_key=ya_public_key,
        ya_public_url=encrypt(ya_public_url, config) if sharing_restricted else ya_public_url,
        sharing_restricted=sharing_restricted,
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