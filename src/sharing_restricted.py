"""
Sharing Restriction Validator Module

This module verifies the consistency of sharing-restricted documents between Yandex.Disk storage
and Google Sheets database. It ensures that documents marked as sharing-restricted are:
1. Properly encrypted in the database
2. Present in both Yandex.Disk and database
3. Located in the correct restricted directory

The module performs several validation checks:
- Validates encryption of document URLs in database
- Cross-references documents between Yandex.Disk and database
- Identifies mismatches and inconsistencies
"""
from yadisk_client import YaDisk
from utils import walk_yadisk, read_config, get_session
from sqlalchemy import select
from utils import Document


def check():
    """
    Validate sharing-restricted documents across storage and database.
    
    This function:
    1. Queries sharing-restricted documents from database
    2. Validates URL encryption for restricted documents
    3. Retrieves sharing-restricted documents from Yandex.Disk
    4. Cross-references documents between storage and database
    5. Reports any inconsistencies found:
       - Documents without encrypted URLs
       - Documents in database but missing from disk
       - Documents in disk but missing from database
    
    Returns:
        None. Prints validation results to console.
    """
    config = read_config()
    with YaDisk(config['yandex']['disk']['oauth_token'], proxy=config['proxy']) as ya_client:
        print("Quering sharing restricted documents in gsheets")
        predicate =(Document.sharing_restricted != False) | (Document.sharing_restricted == True)
        with get_session() as session:
            sharing_restricted_docs_in_gsheets = {d.md5: d for d in session.scalars(select(Document).where(predicate))}
        print(f"Found {len(sharing_restricted_docs_in_gsheets)} sharing restricted docs in gsheets")
        incorrect_docs = []
        for _, doc in sharing_restricted_docs_in_gsheets.items():
            if (doc.ya_public_url and not doc.ya_public_url.startswith("enc:")) or (doc.document_url and not doc.document_url.startswith("enc:")):
                incorrect_docs.append(doc)
                
        if incorrect_docs:
            print("Found docs without encrypted links:")
            for doc in incorrect_docs:
                print(f"md5: '{doc.md5}', ya_url: '{doc.ya_public_url}', doc_url: '{doc.document_url}'")
        
        print("Visiting sharing restricted documents in yandisk")
        sharing_restricted_dir = config['yandex']['disk']['hidden']
        sharing_restricted_docs_in_disk = {d.md5: d for d in walk_yadisk(ya_client, sharing_restricted_dir, fields= ['md5', 'path'])}
        
        docs_in_gsheets_but_not_in_disk = sharing_restricted_docs_in_gsheets.keys() - sharing_restricted_docs_in_disk.keys()
        if docs_in_gsheets_but_not_in_disk:
            print("Found docs in gsheets but not in disk")
            for doc_md5 in docs_in_gsheets_but_not_in_disk:
                print(f">> {sharing_restricted_docs_in_gsheets[doc_md5]}")
                
        docs_in_disk_but_not_in_gsheets = sharing_restricted_docs_in_disk.keys() - sharing_restricted_docs_in_gsheets.keys()
        if docs_in_disk_but_not_in_gsheets:
            print("Found docs in disk but not in sheets")
            for doc_md5 in docs_in_disk_but_not_in_gsheets:
                print(f">> {sharing_restricted_docs_in_disk[doc_md5]}")
        
        print("Checking complete")
                
                
        
        
