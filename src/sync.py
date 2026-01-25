"""
Document Synchronization and Management Module

This module handles synchronization between Yandex.Disk storage and database, manages document 
filtering, and handles deduplication based on various criteria. It provides comprehensive 
document management functionality including content verification, duplicate detection, and 
automated file organization.

Key Features:
1. Document Synchronization
   - Syncs files between Yandex.Disk and database
   - Handles metadata updates and file publishing
   - Manages document visibility and access control

2. Document Filtering
   - Identifies non-Tatar language documents
   - Filters out non-textual content types
   - Handles document deduplication by ISBN
   - Manages restricted content separately

3. Storage Management
   - Handles S3 storage cleanup
   - Manages file movement between directories
   - Updates public sharing links

Constants:
    tatar_bcp_47_codes: List of BCP-47 language codes for Tatar variants
    not_document_types: List of MIME types to be filtered out

Key Functions:
    sync(): Main synchronization process
    _define_docs_for_wiping(): Identifies documents to be removed/moved
    _dedup_by_isbn(): Handles ISBN-based deduplication
    _process_file(): Processes individual files during sync
    _move_to_filtered_out(): Moves files to appropriate filtered directories

Process Flow:
1. Initial Setup
   - Load configuration
   - Connect to S3 and database
   - Retrieve upstream metadata

2. Document Processing
   - Identify documents for removal
   - Process each file in Yandex.Disk
   - Handle duplicates and invalid content
   - Update database records

3. Cleanup
   - Remove filtered content from S3
   - Move files to appropriate directories
   - Update wiping plan

Requirements:
- Yandex.Disk OAuth token
- S3 credentials and bucket configuration
- Database access
- Local storage for wiping plan

Error Handling:
- Graceful handling of API failures
- Transaction safety for database updates
- State persistence for interrupted operations
"""

from utils import read_config, walk_yadisk, encrypt, get_in_workdir, download_file_locally, get_session
from yadisk_client import YaDisk
from rich import print
from models import Document, DocumentCrh
from s3 import  create_session
from sqlalchemy import text, select, delete
import json
from dirs import Dirs
import os
from collections import defaultdict
import pymupdf
import typer
from rich import print
from rich.console import Console
from rich.table import Table


tatar_bcp_47_codes = ['tt-Latn-x-zamanalif', 'tt-Cyrl', 'tt-Latn-x-yanalif', 'tt-Arab', 'tt-Latn']
crimean_tatar_bcp_47_codes = ['crh-Latn', 'crh-Cyrl', 'crh-Latn-x-yanalif', 'crh-Arab']

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
    'application/x-7z-compressed',
    'image/png',
    "image/x-icon",
    "application/x-tplink-bin",
    "video/x-unknown",
    "text/x-Algol68",
    "application/x-chm",
    "video/mp4",
    "image/bmp"
]

def sync():
    """
    Syncs files from Yandex Disk to Google Sheets.
    """
    config = read_config()
    s3client = create_session(config)

    with YaDisk(config['yandex']['disk']['oauth_token'], proxy=config['proxy']) as yaclient: 
        print("Requesting all upstream metadata urls") 
        upstream_metas = _lookup_upstream_metadata(s3client, config)
        print("Requesting all md5s") 
        all_md5s = get_all_md5s(Document)
        all_md5s_crh = get_all_md5s(DocumentCrh)
        all_md5s.update(all_md5s_crh)
        
        print("Defining docs for wiping") 
        docs_for_wiping = _define_docs_for_wiping(yaclient, config) 
        if docs_for_wiping:
            print("Removing objects from s3 storage")
            _remove_from_s3(docs_for_wiping.keys(), s3client, config)
        else:
            print("No docs for wiping found")
            
        print("Syncing yadisk with Google sheets")
        skipped = []
        for lang_tag, entry_point in config['yandex']['disk']['entry_points'].items():
            print(f"Processing entry point '{entry_point}' for language tag '{lang_tag}'")
            for file in walk_yadisk(client=yaclient, root=entry_point):
                try:
                    if dir_to_move := docs_for_wiping.get(file.md5, None):
                        # the file marked for wiping
                        _move_to_filtered_out(file, config, yaclient, dir_to_move, entry_point)
                        # delete record in database
                        with get_session() as session:
                            if doc := session.get(Document, file.md5):
                                session.delete(doc)
                                session.commit()
                        del docs_for_wiping[file.md5]
                        flush(docs_for_wiping)
                    else:
                        meta = upstream_metas.get(file.md5)
                        if doc := _process_file(
                            yaclient, file, all_md5s,
                            skipped, meta, config, lang_tag, entry_point
                        ):
                            with get_session() as session:
                                session.merge(doc)
                                session.commit()

                except Exception as e:
                    import traceback
                    print(f"[red]Error during syncing: {type(e).__name__}: {e} {traceback.format_exc()}[/red]")
            if skipped:
                print("Skipped by MIME type files:")
                print(*skipped, sep="\n")
            
def _move_to_filtered_out(file, config, ya_client, parent_dir, entry_point):
    # For each file
    # 1. move file to dedicated folder
    # 2. unpublish file if it has public link
    filtered_out_dir = config['yandex']['disk']['filtered_out']
    
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

    print("Querying non tatar documents")
    with get_session() as session:
        non_tatar_docs = session.scalars(select(Document).where(Document.language.not_in(tatar_bcp_47_codes)))
        non_tatar_docs = {d.md5: f"nontatar/{'-'.join(sorted(d.language.split(', ')))}" for d in non_tatar_docs}
        print(f"Found {len(non_tatar_docs)} nontatar docs")
        docs_for_wiping.update(non_tatar_docs)
        flush(docs_for_wiping)
        
        print("Querying non textual docs")
        nontextual_docs = session.scalars(select(Document).where(
            Document.mime_type.in_(not_document_types) 
            | 
            Document.ya_path.endswith('.eaf') 
            |
            Document.ya_path.endswith('.musx')
        ))
        nontextual_docs = {d.md5: "nontextual" for d in nontextual_docs}
        print(f"Found {len(nontextual_docs)} nontextual docs")
        docs_for_wiping.update(nontextual_docs)
        flush(docs_for_wiping)
        
        non_crimean_tatar_docs = session.scalars(select(DocumentCrh).where(DocumentCrh.language.not_in(crimean_tatar_bcp_47_codes)))
        non_crimean_tatar_docs = {d.md5: f"noncrimeantatar/{'-'.join(sorted(d.language.split(', ')))}" for d in non_crimean_tatar_docs}
        print(f"Found {len(non_tatar_docs)} noncrimeantatar docs")
        docs_for_wiping.update(non_crimean_tatar_docs)
        flush(docs_for_wiping)
        
        print("Querying non textual docs")
        nontextual_docs = session.scalars(select(DocumentCrh).where(
            DocumentCrh.mime_type.in_(not_document_types) 
            |
            DocumentCrh.ya_path.endswith('.eaf')
            |
            DocumentCrh.ya_path.endswith('.musx')
        ))
        nontextual_docs = {d.md5: "nontextual" for d in nontextual_docs}
        print(f"Found {len(nontextual_docs)} nontextual docs")
        docs_for_wiping.update(nontextual_docs)
        flush(docs_for_wiping)
    
    _dedup_by_isbn(docs_for_wiping, yaclient, config, entity_cls=Document)
    # _dedup_by_isbn(docs_for_wiping, yaclient, config, entity_cls=DocumentCrh)
    
    return docs_for_wiping
    
def _dedup_by_isbn(plan, yaclient, config, entity_cls=Document):
    print("Deduplicating by ISBN")
    # Get all docs that have ISBNs
    with get_session() as session:
        md5s_to_docs = { doc.md5 : doc for doc in  session.scalars(select(entity_cls).where(entity_cls.isbn.is_not(None)))}
    
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
        
    console = Console()
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
            # _hint = []
            table = Table(title=isbn, expand=True, show_lines=True, show_header=False)
            table.add_column("#", justify="center", style="cyan", no_wrap=True)
            table.add_column(" ", )
            _params = set()
            for idx, doc in _choices.items():
                local_path = md5_to_local_path[doc.md5]
                if doc.mime_type in ['application/pdf', 'application/x-pdf']:
                    with pymupdf.open(local_path) as pdf_doc:
                        pages_count = str(pdf_doc.page_count)
                else:
                    pages_count = "N/A"
                size = round(os.path.getsize(local_path) / 1024 / 1024, 2)
                table.add_row(
                    str(idx),
                    f"md5: {doc.md5}\nlocal_path: {local_path}\nya_path: {doc.ya_path}\nsize: {size}\npages_count: {pages_count}\nfull: {doc.full}\nmime_type: {doc.mime_type}\ncontent_url: {doc.content_url if doc.content_url else 'N/A'}",
                )
                _params.add(f"{pages_count}-{size}-{doc.mime_type.strip()}-{doc.full}")
            if len(_params) == 1:
                # all files have same size and pages count, just pick the first
                return _docs - {_choices[1]}
            else:
                # ask user to choose which document to keep
                console.print(table)
                res = typer.prompt(f"Multiple documents with ISBN '{isbn}' found, choose which one to keep", prompt_suffix="> ")
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

        
def _process_file(ya_client, file, all_md5s, skipped_by_mime_type_files, upstream_meta, config, lang_tag, entry_point):
    if file.path.startswith("disk:/НейроТатарлар/kitaplar/monocorpus/Anna's archive/") and file.path.endswith('.txt'):
        print(f"Skipping Anna's archive file '{file.path}'")
        return
    if '/НейроТатарлар/kitaplar/monocorpus/_1st_priority_for_OCR/random_files_thru_yandex_search/ilbyak-school.narod.ru' in file.path and file.path.endswith('.htm'):
        print(f"Skipping ilbyak-school.narod.ru file '{file.path}'")
        return
    
    if '/НейроТатарлар/other_turkic_langs/Крымскотатарский/' in file.path and (
        file.path.endswith('.layout') 
        or
        file.path.endswith('.frw') 
        or
        file.path.endswith('.txt')
        or
        file.path.endswith('.pac')
        or
        file.path.endswith('.csv')
        or
        file.path.endswith('.xml')
        or
        file.path.endswith('.jpg')
        or
        file.path.endswith('.hdr')
        or
        file.path.endswith('.dat')
        or
        file.path.endswith('.frdat')
        or
        file.path.endswith('.vtt')
        or
        file.path.endswith('.ini')
        or
        file.path.endswith('.aux')
        or
        file.path.endswith('.musx')
        or
        file.path.endswith('.mxl')
        or
        file.path.endswith('.zip')
        or
        file.path.endswith('.indd')
        or
        file.path.endswith('.swp')
        or
        file.path.endswith('.tmp')
        or
        file.path.endswith('.DS_Store')
        or 
        file.path.endswith('.parquet')
        or 
        file.path.endswith('.emf')
        or 
        file.path.endswith('.json')
    ):
        print(f"Skipping crimean tatar layout file '{file.path}'")
        return

    _should_be_skipped, mime_type = should_be_skipped(file)
    if _should_be_skipped:
        _move_to_filtered_out(file, config, ya_client, 'nontextual', entry_point)
        skipped_by_mime_type_files.append((file.mime_type, file.public_url, file.path))
        return
    
    ya_public_key = file.public_key
    ya_public_url = file.public_url
    if not (ya_public_key and ya_public_url):
        ya_public_key, ya_public_url = _publish_file(ya_client, file.path)
    
    ya_path = file.path.removeprefix('disk:')    
    if file.md5 in all_md5s:
        # compare with ya_resource_id
        # if 'resource_id' is the same, then skip, due to we have it in gsheet
        # if not, then remove from yadisk due to it is duplicate
        if all_md5s[file.md5]['resource_id'] != file.resource_id:
            print(f"File '{file.path}' already exists in gsheet, but with different resource_id: '{file.resource_id}' with md5 '{file.md5}', removing it from yadisk")
            ya_client.remove(file.path, md5=file.md5)
            return
        # if md5 is the same but path or ya_public_url is different, proceed to updating
        if (all_md5s[file.md5]['ya_path'] == ya_path 
            # and 
            # (
            #     (sharing_restricted and all_md5s[file.md5]['ya_public_url'] == encrypt(ya_public_url, config))
            #     or
            #     (not sharing_restricted and all_md5s[file.md5]['ya_public_url'] == ya_public_url))
            ):
            return
        
    print(f"[green]Adding file to gsheets '{file.path}' with md5 '{file.md5}'[/green]")

    sharing_restricted = config["yandex"]["disk"]["hidden"] in file.path 
    doc = Document() if lang_tag == 'tt' else DocumentCrh()
    doc.md5=file.md5
    doc.mime_type=mime_type
    doc.ya_path=ya_path
    doc.ya_public_key=ya_public_key
    doc.ya_public_url=encrypt(ya_public_url, config) if sharing_restricted else ya_public_url
    doc.sharing_restricted=sharing_restricted
    doc.ya_resource_id=file.resource_id
    doc.upstream_meta_url=upstream_meta
    doc.full=False if "милли.китапханә/limited" in file.path else True
    # update gsheet
    all_md5s[file.md5] = {"resource_id": doc.ya_resource_id, "upstream_meta_url": doc.upstream_meta_url} 
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
    
def get_all_md5s(entity_cls):
    """
    Returns a dict of all md5s in the database with ya_resource_id
    :return: set of md5s
    """
    with get_session() as session:
        res = session.execute(
            select(entity_cls.md5, entity_cls.ya_resource_id, entity_cls.upstream_meta_url, entity_cls.ya_path, entity_cls.ya_public_url)
        ).all()
        return { 
                i[0]: {"resource_id": i[1], "upstream_meta_url": i[2], "ya_path": i[3], "ya_public_url": i[4]} 
                for i 
                in res 
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