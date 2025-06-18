from yadisk_client import YaDisk
from utils import read_config, obtain_documents, download_file_locally, get_in_workdir
from monocorpus_models import Document, Session
from ebooklib import epub, ITEM_NAVIGATION, ITEM_DOCUMENT, ITEM_IMAGE, ITEM_STYLE, ITEM_FONT, ITEM_COVER, ITEM_UNKNOWN
from bs4 import BeautifulSoup, NavigableString
from markdownify import markdownify as md
import mdformat
from dirs import Dirs
from rich import print
import re
from s3 import upload_file, create_session
import os
import zipfile
from urllib.parse import urlparse
from rich import print
from sqlalchemy import select
import time
from markitdown import MarkItDown

def extract():
    config = read_config()
    predicate = (
        Document.content_url.is_(None) &
        Document.mime_type.is_('application/vnd.openxmlformats-officedocument.wordprocessingml.document') # docx
    )
    s3session = create_session(config)
    with YaDisk(config['yandex']['disk']['oauth_token']) as ya_client, Session() as write_session, Session() as read_session:
        docs = read_session.query(select(Document).where(predicate).limit(1))
        if not docs:
            print("No documents for processing...")
            return
        for doc in docs:
            try:
                _extract_content(doc, config, ya_client, s3session, write_session)
            except KeyboardInterrupt:
                exit()
            except BaseException as e:
                print(f"Could not extract metadata from doc {doc.md5}: {e}")
                    
def _extract_content(doc, config, ya_client, s3session, gsheet_session):
    print(f"Extracting content from file {doc.md5}({doc.file_name})")
    local_doc_path = download_file_locally(ya_client, doc, config)
    md = MarkItDown(enable_plugins=False) # Set to True to enable plugins
    content = md.convert(local_doc_path)

    formatted_content = mdformat.text(
        content.markdown,
        codeformatters=(),
        extensions=["toc", "footnote"],
        options={"wrap": "keep", "number": "keep", "validate": True, "end_of_line": "lf"},
    )
    formatted_response_md = get_in_workdir(Dirs.CONTENT, file=f"{doc.md5}-formatted.md")
    with open(formatted_response_md, 'w') as f:
        f.write(formatted_content)
        
    content_bucket = config["yandex"]["cloud"]['bucket']['content']
    content_key = f"{doc.md5}.zip"
    local_content_path = get_in_workdir(Dirs.CONTENT, file=content_key)
    with zipfile.ZipFile(local_content_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
        zf.write(arcname=f"{doc.md5}.md", filename=formatted_response_md)
    doc.content_url = upload_file(local_content_path, content_bucket, content_key, s3session)

    document_bucket = config["yandex"]["cloud"]['bucket']['document']
    doc_key = os.path.basename(local_doc_path)
    doc.document_url = upload_file(local_doc_path, document_bucket, doc_key, s3session, skip_if_exists=True)
    doc.content_extraction_method = "markitdown"

    gsheet_session.update(doc)