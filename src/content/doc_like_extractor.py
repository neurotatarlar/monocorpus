from utils import get_in_workdir
from dirs import Dirs
from rich import print
import re
import os
from rich import print
import subprocess
import chardet
import subprocess
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from googleapiclient.discovery import build
import io
import shutil


# these formats requires preformatting into docx format before extraction
to_docx_mime_types = set([
    'text/rtf',
    'application/rtf',
    'application/rtf+xml',
    'application/msword',
    'text/rtf',
    'application/x-rtf',
    'application/vnd.oasis.opendocument.text',
])


# these formats requires checking to UTF-8 format 
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


gdrive_operative_folder_name = '1WFYCcbrtKGv3KTwyKdcKHKxXwmr9iFHE'


class DocLikeExtractor:
    def __init__(self, doc, local_doc_path, config, s3lient, gcloud_creds):
        self.doc = doc
        self.local_doc_path = local_doc_path
        self.config = config
        self.s3lient = s3lient
        self.gcloud_creds = gcloud_creds
        
    def extract(self):
        response_path = get_in_workdir(Dirs.CONTENT, file=f"{self.doc.md5}-formatted.md")
        
        if self.doc.mime_type == 'text/markdown':
            shutil.copyfile(self.local_doc_path, response_path)
        else:
            self._preprocess_if_required()
        
            cmd = [
                "pandoc",
                self.local_doc_path,
                "-o", response_path,
                "-t", "markdown_mmd",
                # "--extract-media", clips_dir,
                "--wrap=preserve"
            ]
        
            subprocess.run(cmd, check=True)
        
            return self._postprocess(response_path)

            
    def _preprocess_if_required(self):
        if self.doc.mime_type in check_encoding_mime_types:
            # Step 1: Detect encoding
            with open(self.local_doc_path, 'rb') as f:
                raw_data = f.read()
                detected = chardet.detect(raw_data)
                encoding = detected['encoding']

            # Step 2: Convert to UTF-8 if needed
            if encoding.lower() != 'utf-8':
                print(f"Converting {self.local_doc_path} from {encoding} to UTF-8...")
                text = raw_data.decode(encoding)
                with open(self.local_doc_path, 'w', encoding='utf-8') as f:
                    f.write(text)
        
        if self.doc.mime_type in to_docx_mime_types:
            service = build('drive', 'v3', credentials=self.gcloud_creds)
            file_metadata = {
                'name': os.path.basename(self.local_doc_path), 
                'mimeType': 'application/vnd.google-apps.document',
                'parents': [gdrive_operative_folder_name]  # Use a specific folder for conversion,
            }
            media = MediaFileUpload(self.local_doc_path, resumable=True)
            uploaded = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
            file_id = uploaded.get('id')

            output_path = get_in_workdir(Dirs.ENTRY_POINT, file=f"{self.doc.md5}.docx")
            request = service.files().export_media(fileId=file_id,
                mimeType='application/vnd.openxmlformats-officedocument.wordprocessingml.document')
            fh = io.FileIO(output_path, 'wb')
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while done is False:
                _, done = downloader.next_chunk()
            
            service.files().delete(fileId=file_id).execute()
            self.local_doc_path = output_path
    
    def _postprocess(self, response_path):
        with open(response_path, 'r') as f:
            content = f.read()
            
        content = re.sub(r"^!\[\]\(.*?\)\s*", '', content, flags=re.MULTILINE)
        content = re.sub(r'^- ?', '— ', content, flags=re.MULTILINE)
        content = re.sub(r'!\[.*?\]\(.*?\)', '', content, flags=re.MULTILINE)
        content = re.sub(r'<img\s+[^>]*src="[^"]+"[^>]*>', '', content)
        content = re.sub(r'dN=`?.*?</a>`?\{=html\}', '', content, flags=re.DOTALL)
        
        return content
            
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

