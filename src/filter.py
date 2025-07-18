# import os
# from monocorpus_models import Document, Session
# from utils import read_config, walk_yadisk, get_in_workdir, download_file_locally
# from sqlalchemy import select
# from yadisk_client import YaDisk
# from s3 import  create_session
# from rich import print
# from sqlalchemy import delete
# import typer
# import json
# from collections import defaultdict
# from dirs import Dirs
# from sqlalchemy import text, select
# import pymupdf





# def filter():
#     config = read_config()
#     docs_for_wiping = get_plan()
        
#     with Session() as session, YaDisk(config['yandex']['disk']['oauth_token']) as yaclient: 
#         session.query(text("select 1"))
#         print("Querying non tatar documents")
#         non_tatar_docs = Session().query(select(Document).where(Document.language.not_in(tatar_bcp_47_codes)))
#         non_tatar_docs = {d.md5: f"nontatar/{'-'.join(sorted(d.language.split(', ')))}" for d in non_tatar_docs}
#         print(f"Found {len(non_tatar_docs)} nontatar docs")
#         docs_for_wiping.update(non_tatar_docs)
#         flush(docs_for_wiping)
        
#         print("Querying non textual docs")
#         nontextual_docs = Session().query(select(Document).where(Document.mime_type.in_(not_document_types)))
#         nontextual_docs = {d.md5: "nontextual" for d in nontextual_docs}
#         print(f"Found {len(nontextual_docs)} nontextual docs")
#         docs_for_wiping.update(nontextual_docs)
#         flush(docs_for_wiping)
        
#         dedup_by_isbn(docs_for_wiping, yaclient, config)
        
#         if not docs_for_wiping:
#             print("No docs for wiping found, exiting...")
#             return
        
#         exit()

        
#         print("Moving and unpublishing files")
#         entry_point = config['yandex']['disk']['entry_point']
#         for file in walk_yadisk(client=yaclient, root=entry_point, fields=["path", "md5"]):
#             if dir_to_move := docs_for_wiping.get(file.md5, None):
#                 print(f"Moving file '{file.path}' and removing from Google Sheets")
#                 move_to_filtered_out(file, config, yaclient, dir_to_move)
#                 session._get_session().execute(delete(Document).where(Document.md5.is_(file.md5)))
#                 del docs_for_wiping[file.md5]
#                 flush(docs_for_wiping)
#             if not docs_for_wiping:
#                 print("No more files to wipe, exiting...")
#                 break

# def dedup_by_isbn(plan, yaclient, config):
#     print("Deduplicating by ISBN")
#     # Get all docs that have ISBNs
#     md5s_to_docs = { doc.md5 : doc for doc in  Session().query(select(Document).where(Document.isbn.is_not(None)))}
    
#     # Group them by ISBN
#     isbns_to_docs = defaultdict(set)
#     for doc in md5s_to_docs.values():
#         isbns = doc.isbn.strip().split(',')
#         isbns = ", ".join(sorted([isbn.strip() for isbn in isbns if isbn.strip()]))
#         isbns_to_docs[isbns].add(doc.md5)
            
#     # Find duplicates
#     duplicated_isbn_to_md5s = defaultdict(set)
#     duplicated_docs_md5s = set()
#     for isbn, md5s in isbns_to_docs.items():
#         if len(md5s) > 1 and isbn:
#             print(f"Found duplicate ISBN: '{isbn}' with md5s {md5s}")
#             duplicated_isbn_to_md5s[isbn].update(md5s)
#             duplicated_docs_md5s.update(md5s)
#     del isbns_to_docs
        
#     if not duplicated_isbn_to_md5s:
#         print("No duplicate ISBNs found, exiting...")
#         return
    
#     print(f"Downloading books with {len(duplicated_docs_md5s)} duplicate ISBNs")
#     md5_to_local_path = {
#         doc_md5: download_file_locally(yaclient, md5s_to_docs[doc_md5], config)
#         for doc_md5
#         in duplicated_docs_md5s
#     }
#     del duplicated_docs_md5s
        
#     for isbn, md5s in duplicated_isbn_to_md5s.items():
        
#         def _define_docs_to_move(_docs):
#             _full_docs = set([d for d in _docs if d.full == True])
#             # if we have only one full document among duplicates then keep it and move anothers
#             if len(_full_docs) == 1:
#                 return _docs - _full_docs
#             _pdf_docs = set([d for d in _docs if d.mime_type in ['application/pdf', 'application/x-pdf'] and d.full == True])
#             # if we have exactly one full pdf among duplicates then keep it and move anothers
#             if len(_pdf_docs) == 1:
#                 return _docs - _pdf_docs
#             _extracted_pdf_docs = set([d for d in _pdf_docs if d.content_url])
#             #  if we have multiple pdf docs, but only one of them already extracted then keep it and move anothers
#             if len(_extracted_pdf_docs):
#                 return _docs - _extracted_pdf_docs
            
#             _choices = {idx: doc for idx, doc in enumerate(sorted(_docs, key=lambda d: d.ya_public_url), start=1)}
#             _hint = []
#             _params = set()
#             for idx, doc in _choices.items():
#                 local_path = md5_to_local_path[doc.md5]
#                 if doc.mime_type in ['application/pdf', 'application/x-pdf']:
#                     with pymupdf.open(local_path) as pdf_doc:
#                         pages_count = str(pdf_doc.page_count)
#                 else:
#                     pages_count = "N/A"
#                 size = round(os.path.getsize(local_path) / 1024 / 1024, 2)
#                 _hint.append(f"{idx}: {doc.md5} '{local_path}' {size} {pages_count} {doc.full} {doc.mime_type} {f' {doc.content_url}' if doc.content_url else ''}")
#                 _params.add(f"{pages_count}-{size}-{doc.mime_type.strip()}-{doc.full}")
#             if len(_params) == 1:
#                 # all files have same size and pages count, just pick the first
#                 return _docs - {_choices[1]}
#             else:
#                 # ask user to choose which document to keep
#                 _hint = "\n".join(_hint)
#                 res = typer.prompt(f"Multiple documents with ISBN '{isbn}' found, choose which one to keep:\n{_hint}\n", prompt_suffix="> ")
#                 if res.isdigit() and int(res) in _choices:
#                     return _docs - {_choices[int(res)]}
#                 else:
#                     print(f"Invalid choice '{res}', skipping ISBN {isbn}")
#                     return None

#         docs_same_isbn = {md5s_to_docs[md5] for md5 in md5s}
#         docs_for_wiping = _define_docs_to_move(docs_same_isbn)
#         if docs_for_wiping:
#             plan.update({d.md5: f"duplicated_isbn/{isbn}" for d in docs_for_wiping})
#             flush(plan)
        

    

# def move_to_filtered_out(file, config, ya_client, parent_dir):
#     # For each file
#     # 1. move file to dedicated folder
#     # 2. unpublish file if it has public link
#     filtered_out_dir = config['yandex']['disk']['filtered_out']
#     entry_point = config['yandex']['disk']['entry_point']
    
#     if parent_dir == 'void':
#         ya_client.remove(file.path, n_retries=5, retry_interval=30)
#     else:
#         old_path = file.path.removeprefix('disk:')
#         _rel_path = os.path.relpath(old_path, entry_point)
#         new_path = os.path.join(filtered_out_dir, parent_dir, _rel_path)
#         if old_path != new_path:
#             ya_client.create_folders(os.path.dirname(new_path))
#             ya_client.move(file.path, new_path, n_retries=5, retry_interval=30, overwrite=True)
#         ya_client.unpublish(new_path)
    
# def _remove_from_s3(md5s, s3client, config):
#     if not md5s:
#         return
#     content_bucket = config["yandex"]["cloud"]['bucket']['content']
#     content_chunks_bucket = config["yandex"]["cloud"]['bucket']['content_chunks']
#     documents_bucket = config["yandex"]["cloud"]['bucket']['document']
#     images_bucket = config["yandex"]["cloud"]['bucket']['image']
#     upstream_metadatas_bucket = config["yandex"]["cloud"]['bucket']['upstream_metadata']
#     metadatas_bucket = config["yandex"]["cloud"]['bucket']['metadata']
#     buckets = [content_bucket, content_chunks_bucket, documents_bucket, images_bucket, upstream_metadatas_bucket, metadatas_bucket]
#     for bucket in buckets:
#         paginator = s3client.get_paginator('list_objects_v2')
#         pages = paginator.paginate(Bucket=bucket)

#         keys_to_remove = []
#         for page in pages:
#             for obj in page.get('Contents', []):
#                 key = obj['Key']
#                 if any(key.startswith(md5) for md5 in md5s):
#                     keys_to_remove.append({'Key': key})
                    
#         if keys_to_remove:
#             print(f"Removing {len(keys_to_remove)} objects from bucket '{bucket}'")
#             for i in range(0, len(keys_to_remove), 1000):
#                 batch = keys_to_remove[i:i+1000]
#                 s3client.delete_objects(Bucket=bucket, Delete={'Objects': batch, 'Quiet': True})
