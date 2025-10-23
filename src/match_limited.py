"""
Limited Documents Matching and Migration Module

This module handles the identification and migration of documents between limited and fully downloaded 
collections in Yandex.Disk, along with their corresponding metadata in S3 storage.

Key Features:
1. Document Matching
   - Compares documents between limited and fully downloaded directories
   - Normalizes filenames for accurate matching
   - Identifies overlapping documents between collections

2. Metadata Migration
   - Transfers upstream metadata between S3 objects
   - Updates metadata references using new document hashes
   - Maintains wiping plan for obsolete documents

3. State Management
   - Tracks documents marked for wiping
   - Persists state between runs
   - Handles incremental updates

Functions:
    match_limited(): Main entry point for matching and migration process
    _lookup_upstream_metadata(): Retrieves existing metadata from S3
    _get_wiping_plan(): Loads or creates document wiping state
    _flush(): Persists updated wiping plan

Directory Structure:
    limited_dir: "/НейроТатарлар/kitaplar/monocorpus/милли.китапханә/limited"
        Contains documents with limited/incomplete content

    downloaded_fully_dir: "/НейроТатарлар/kitaplar/monocorpus/_1st_priority_for_OCR/milli_kitaphana_(un)limited"
        Contains complete versions of previously limited documents

Process Flow:
1. Scan both directories and normalize document names
2. Identify overlapping documents
3. Lookup existing metadata in S3
4. For each match:
   - Copy metadata to new location with updated hash
   - Remove old metadata
   - Mark old document for wiping
5. Save updated wiping plan

Requirements:
- Yandex.Disk access token
- S3 credentials and bucket configuration
- Local storage for wiping plan
"""
from utils import walk_yadisk, read_config, get_in_workdir
import json 
from yadisk_client import YaDisk
import unicodedata
from s3 import  create_session
from dirs import Dirs
import os


limited_dir = "/НейроТатарлар/kitaplar/monocorpus/милли.китапханә/limited"
downloaded_fully_dir = "/НейроТатарлар/kitaplar/monocorpus/_1st_priority_for_OCR/milli_kitaphana_(un)limited" 

def match_limited():
    config = read_config()
    with YaDisk(config['yandex']['disk']['oauth_token']) as ya_client: 
        limited_docs = {unicodedata.normalize("NFC", d.name.strip()): d for d in walk_yadisk(ya_client, limited_dir, fields= ['name', 'md5'])}
        print(f"Got {len(limited_docs)} docs in dir with limited(non-complete) docs")
        
        downloaded_fully_docs = {unicodedata.normalize("NFC", d.name.strip()): d for d in walk_yadisk(ya_client, downloaded_fully_dir, fields= ['name', 'md5'])}
        print(f"Got {len(downloaded_fully_docs)} docs in dir with limited but fully downladed docs")
        
        intersected = downloaded_fully_docs.keys() & limited_docs.keys()
        if not intersected:
            print("No intersections found, exiting...")
            return
        
        s3client = create_session(config)
        upstream_metadata_bucket = config['yandex']['cloud']['bucket']['upstream_metadata']
        upstream_metas = _lookup_upstream_metadata(s3client, upstream_metadata_bucket)
        docs_for_wiping = _get_wiping_plan()

        for doc_name in intersected:
            doc_in_limited_docs = limited_docs[doc_name].md5
            full_doc = downloaded_fully_docs[doc_name].md5
            if upstream_meta := upstream_metas.get(doc_in_limited_docs):
                print(f"Found upstream metadata {doc_in_limited_docs}: {upstream_meta}")
                old_key = f"{doc_in_limited_docs}.zip"
                new_key = f"{full_doc}.zip"
                
                # Step 1: Copy
                s3client.copy_object(
                    Bucket=upstream_metadata_bucket,
                    CopySource={'Bucket': upstream_metadata_bucket, 'Key': old_key},
                    Key=new_key
                )

                # Step 2: Delete old object
                s3client.delete_object(Bucket=upstream_metadata_bucket, Key=old_key)
            docs_for_wiping[doc_in_limited_docs] = "void"
        _flush(docs_for_wiping)
            
        
def _lookup_upstream_metadata(s3client, bucket):
    s3client.list_objects_v2(Bucket=bucket)
    paginator = s3client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket)
    return {
         obj['Key'].removesuffix('.zip'): f"{s3client._endpoint.host}/{bucket}/{obj['Key']}"
         for page in pages
         for obj in page['Contents']
    }
    
    
def _get_wiping_plan():
    marked_for_wiping = get_in_workdir(Dirs.WIPING_PLAN, file="marked_for_wiping.json")
    if not os.path.exists(marked_for_wiping):
        print("No marked for wiping file found, creating a new one")
        with open(marked_for_wiping, 'w') as f:
            json.dump({}, f)
    with open(marked_for_wiping, 'r') as f:
        return json.load(f)
    
    
def _flush(plan):
    marked_for_wiping = get_in_workdir(Dirs.WIPING_PLAN, file="marked_for_wiping.json")
    with open(marked_for_wiping, 'w') as f:
        json.dump(plan, f, indent=4, ensure_ascii=False)