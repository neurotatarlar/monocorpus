# from monocorpus_models import Document, Session
# from sqlalchemy import select
# from utils import encrypt, decrypt, read_config

# def hide():
#     config = read_config()
#     write_session = Session()
#     read_session = Session()
#     for doc in read_session.query(select(Document).where(Document.sharing_restricted.is_(True))):
#         if not doc.ya_public_url.startswith('enc:'):
#             raise ValueError(f"Doc {doc.md5} has no public url")
#         if doc.document_url and not doc.document_url.startswith('enc:'):
#             ciphertext = encrypt(doc.document_url, config)
#             assert decrypt(ciphertext, config) == doc.document_url, "Encrypted and decrypted did not match"
#             print(f"Updating doc {doc.md5}, {doc.document_url}, {ciphertext}")
#             doc.document_url = ciphertext
#             write_session.update(doc)
            
#     print("complete")
    
