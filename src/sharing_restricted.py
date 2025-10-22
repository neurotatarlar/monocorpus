from yadisk_client import YaDisk
from utils import walk_yadisk, read_config, get_session
from sqlalchemy import select
from utils import Document


def check():
    config = read_config()
    with YaDisk(config['yandex']['disk']['oauth_token']) as ya_client, get_session() as gsheet_session:
        print("Quering sharing restricted documents in gsheets")
        predicate = Document.sharing_restricted.is_not(False) | Document.sharing_restricted.is_(True)
        sharing_restricted_docs_in_gsheets = {d.md5: d for d in gsheet_session.query(select(Document).where(predicate))}
        print(f"Found {len(sharing_restricted_docs_in_gsheets)} sharing restricted docs in gsheets")
        incorrect_docs = []
        for _, doc in sharing_restricted_docs_in_gsheets.items():
            if not (doc.ya_public_url.startswith("enc:") and doc.document_url.startswith("enc:")):
                incorrect_docs.append(doc)
                
        if incorrect_docs:
            print("Found docs without encrypted links:")
            for doc in incorrect_docs:
                print(f"md5: '{doc.md5}', ya_url: '{doc.ya_public_url}', doc_url: '{doc.document_url}'")
            return
        
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
                
                
        
        
