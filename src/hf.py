"""
Hugging Face Dataset Assembly Module

This module handles the creation of a structured dataset from content files stored in Yandex Cloud,
preparing them for use with Hugging Face's datasets library. 

The module processes Tatar language documents by:
1. Downloading content files from Yandex Cloud storage
2. Matching documents with metadata from the database
3. Extracting text content from zip archives
4. Assembling a structured dataset with metadata
5. Exporting the final dataset to parquet format

The resulting dataset includes document ID (MD5 hash), publication year, genre, and full text content.
"""

from rich import print 
import os
from utils import read_config, get_in_workdir, get_session
from s3 import download
from dirs import Dirs
import pandas as pd
import zipfile
from sqlalchemy import select
from rich import print
from models import Document
from rich.progress import track


def assemble_dataset():
    """
    Assemble a structured dataset from content files.
    
    This function:
    1. Retrieves document metadata from the database
    2. Downloads and processes content files from Yandex Cloud
    3. Extracts text content from zip archives
    4. Creates a structured dataset with metadata
    5. Exports the dataset to parquet format
    
    The function tracks:
    - Empty documents (skipped)
    - Documents missing from the database
    - Processing progress
    
    Returns:
        None. Outputs a parquet file with the assembled dataset.
    """
    print("Assembling structured dataset from content files...")
    config = read_config()
    output_dir = get_in_workdir(Dirs.CONTENT)
    rows = []
    
    # Get all documents with content from database
    with get_session() as session:
        docs = {doc.md5 : doc for doc in session.scalars(select(Document).where(Document.content_url.is_not(None)))}
        
    empty_docs = set()
    not_in_gsheets = set()
    
    # Process each content file
    for content_file in track(download(bucket=config['yandex']['cloud']['bucket']['content'], 
                                     download_dir=output_dir), 
                            description="Processing documents"):
        md5, _ = os.path.splitext(os.path.basename(content_file))
        
        # Check if document exists in database
        if not (doc := docs.get(md5)):
            print(f"No matching document with md5 {md5}, skipping it...")
            not_in_gsheets.add(md5)
            continue
        
        # Extract content from zip file
        with zipfile.ZipFile(content_file, 'r') as zf:
            _md_files = [f for f in zf.namelist()]
            if len(_md_files) != 1:
                raise ValueError(f"Expected exactly one markdown file in the zip, found {len(_md_files)}")
            md_file = _md_files[0]
            content = zf.read(md_file)
            
            # Skip empty content
            if not content:
                empty_docs.add(doc.md5)
                print(f"Content is empty for document {doc.md5}, skipping it...")
                continue
                
            # Add document to dataset
            rows.append({
                "id": md5,
                "publish_year": int(doc.publish_date) if doc.publish_date else None,
                "genre" : doc.genre,
                "text": content.decode('utf-8')
            })
    
    # Create and save dataset        
    df = pd.DataFrame(rows)
    print(f"Final dataset size: {df.shape[0]} documents")
    print("Exporting to parquet...")
    result_file = get_in_workdir(file="tatar_structured_content.parquet")
    df['publish_year'] = df['publish_year'].astype('UInt16')
    df.to_parquet(result_file, index=False)
    print(f"✅ Exported to '{result_file}'")    
    
    # Report skipped documents
    if empty_docs:
        print("Empty documents:")
        for doc in empty_docs:
            print(doc)
        
    if not_in_gsheets:
        print("Docs not present in gheets:")
        for doc in not_in_gsheets:
            print(doc)