from yadisk_client import YaDisk
from utils import read_config, download_file_locally, get_in_workdir
from monocorpus_models import Document, Session, SCOPES
import mdformat
from dirs import Dirs
from rich import print
import re
from s3 import upload_file, create_session
import os
import zipfile
from rich import print
from sqlalchemy import select
import subprocess
import chardet
import subprocess
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from googleapiclient.discovery import build
import io
import shutil
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow


to_docx_mime_types = set([
    'text/rtf',
    'application/rtf',
    'application/rtf+xml',
    'application/msword',
    'text/rtf',
    'application/x-rtf',
    'application/vnd.oasis.opendocument.text',
])

check_encoding_mime_types = set([
    "text/plain",
    "text/csv",
    "text/tab-separated-values",
    "application/xml",
    "text/xml",
    "text/markdown",
    "application/x-tex",
    "text/x-tex",
    "application/x-subrip",
    "application/json",
    "application/x-yaml",
    "text/yaml",
    "text/x-ini"
])

all_mime_types = to_docx_mime_types | check_encoding_mime_types | set(['application/vnd.openxmlformats-officedocument.wordprocessingml.document']) | set(["text/markdown"])

def extract():
    config = read_config()
    predicate = (
        Document.content_url.is_(None) &
        Document.mime_type.in_(all_mime_types) & 
        Document.full.is_(True)
    )
    s3session = create_session(config)
    creds = _get_credentials()
    
    with YaDisk(config['yandex']['disk']['oauth_token']) as ya_client, Session() as write_session, Session() as read_session:
        docs = read_session.query(select(Document).where(predicate))
        if not docs:
            print("No documents for processing...")
            return
        for doc in docs:
            try:
                _extract_content(doc, config, ya_client, s3session, write_session, creds)
            except KeyboardInterrupt:
                exit()
            except BaseException as e:
                print(f"Could not extract content from doc {doc.md5}: {e}")
            
def _get_credentials():
    token_file = "personal_token.json"
    
    if os.path.exists(token_file):
        return Credentials.from_authorized_user_file(token_file, SCOPES)
    
    flow = InstalledAppFlow.from_client_secrets_file('client_secret.json', SCOPES)
    creds = flow.run_local_server(port=0)
    with open(token_file, 'w') as f:
        f.write(creds.to_json())
    return Credentials.from_authorized_user_file(token_file, SCOPES)
                    
def _extract_content(doc, config, ya_client, s3session, gsheet_session, creds):
    print(f"Extracting content from file {doc.md5}({doc.file_name})")
    local_doc_path = download_file_locally(ya_client, doc, config)
    response_path = get_in_workdir(Dirs.CONTENT, file=f"{doc.md5}-formatted.md")
    
    if doc.mime_type == 'text/markdown':
        shutil.copyfile(local_doc_path, response_path)
    else:
        local_doc_path = _preprocess_if_required(doc, local_doc_path, creds)
    
        cmd = [
            "pandoc",
            local_doc_path,
            "-o", response_path,
            "-t", "markdown_mmd",
            # "--extract-media", clips_dir,
            "--wrap=preserve"
        ]
    
        subprocess.run(cmd, check=True)
    
        _postprocess(response_path)
    
    mdformat.file(
        response_path,
        codeformatters=(),
        extensions=["toc", "footnote"],
        options={"wrap": "keep", "number": "keep", "validate": True, "end_of_line": "lf"},
    )

    content_bucket = config["yandex"]["cloud"]['bucket']['content']
    content_key = f"{doc.md5}.zip"
    local_content_path = get_in_workdir(Dirs.CONTENT, file=content_key)
    with zipfile.ZipFile(local_content_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
        zf.write(arcname=f"{doc.md5}.md", filename=response_path)
    doc.content_url = upload_file(local_content_path, content_bucket, content_key, s3session)

    document_bucket = config["yandex"]["cloud"]['bucket']['document']
    doc_key = os.path.basename(local_doc_path)
    doc.document_url = upload_file(local_doc_path, document_bucket, doc_key, s3session, skip_if_exists=True)
    doc.content_extraction_method = "pandoc"

    gsheet_session.update(doc)
    
def _preprocess_if_required(doc, path, creds):
    _encode_if_required(doc, path)
    path = convert_to_docx_if_required(doc, path, creds)
    return path
    
def convert_to_docx_if_required(doc, path, creds):
    if doc.mime_type in to_docx_mime_types:
        service = build('drive', 'v3', credentials=creds)
        file_metadata = {
            'name': os.path.basename(path), 
            'mimeType': 'application/vnd.google-apps.document',
            'parents': ['1WFYCcbrtKGv3KTwyKdcKHKxXwmr9iFHE']  # Use a specific folder for conversion,
        }
        media = MediaFileUpload(path, resumable=True)
        uploaded = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        file_id = uploaded.get('id')

        output_path = get_in_workdir(Dirs.ENTRY_POINT, file=f"{doc.md5}.docx")
        request = service.files().export_media(fileId=file_id,
            mimeType='application/vnd.openxmlformats-officedocument.wordprocessingml.document')
        fh = io.FileIO(output_path, 'wb')
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            _, done = downloader.next_chunk()
        
        service.files().delete(fileId=file_id).execute()
        path = output_path
            
    return path
    
def _encode_if_required(doc, path):
    if doc.mime_type in check_encoding_mime_types:
        # Step 1: Detect encoding
        with open(path, 'rb') as f:
            raw_data = f.read()
            detected = chardet.detect(raw_data)
            encoding = detected['encoding']

        # Step 2: Convert to UTF-8 if needed
        if encoding.lower() != 'utf-8':
            print(f"Converting {path} from {encoding} to UTF-8...")
            text = raw_data.decode(encoding)
            with open(path, 'w', encoding='utf-8') as f:
                f.write(text)
    
    
        
    
def _postprocess(response_path):
    with open(response_path, 'r') as f:
        content = f.read()
        
    content = re.sub(r"^!\[\]\(.*?\)\s*", '', content, flags=re.MULTILINE)
    content = re.sub(r'^- ?', '— ', content, flags=re.MULTILINE)
    content = re.sub(r'!\[.*?\]\(.*?\)', '', content, flags=re.MULTILINE)
    content = re.sub(r'<img\s+[^>]*src="[^"]+"[^>]*>', '', content)
    content = re.sub(r'dN=`?.*?</a>`?\{=html\}', '', content, flags=re.DOTALL)
    
    with open(response_path, 'w', encoding='utf-8') as f:
        f.write(content)
        
    # def __replacer(match):
    #     src_path = match.group(1)
    #     # Extract hash from path
    #     # Looks for /{hash}/media/{filename}
    #     m = re.search(r'/([a-f0-9]{32})/media/', src_path)
    #     if not m:
    #         return match.group(0)  # leave unchanged if no hash
    #     img_hash = m.group(1)

    #     s3_filename = f"{img_hash}-{counter}.jpg"
    #     s3_url = f"https://storage.yandexcloud.net/ttimg/{s3_filename}"

    #     figure_id = f"{img_hash}-{counter}"
    #     figure_html = (
    #         f'<figure style="text-align: center; margin: 1em 0;" id="{figure_id}">'
    #         f'<img alt="" src="{s3_url}" style="max-width: 800px; width: 50%; height: auto;">'
    #         f'</figure>'
    #     )
    #     counter += 1
    #     return figure_html

    # detect and replace all images with links