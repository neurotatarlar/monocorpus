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


class EpubExtractor:
    
    def __init__(self, doc, local_doc_path, config, s3lient):
        self.doc = doc
        self.local_doc_path = local_doc_path
        self.config = config
        self.s3lient = s3lient
    
    
    def extract(self):
        print(f"Extracting content from file {self.doc.md5}({self.doc.file_name})")
        md_content, icr = self._extract(self.doc, self.config, self.local_doc_path, s3lient) 
        return self._postprocess(md_content)


    def _postprocess(self, content):
        content = re.sub(r"^xml version='1\.0' encoding='utf-8'\?\s*", '', content, flags=re.MULTILINE)
        content = re.sub(r"^!\[\]\(.*?\)\s*", '', content, flags=re.MULTILINE)
        content = re.sub(r'^- ?', '— ', content, flags=re.MULTILINE)
        content = re.sub(r'!\[.*?\]\(.*?\)', '', content, flags=re.MULTILINE)
        return content
        
        
    def _extract(self, doc, config, local_doc_path, s3session):
        outputs = []
        book = epub.read_epub(local_doc_path)
        clips_counter = 0
        clips_dir = get_in_workdir(Dirs.CLIPS)
        clips_bucket = config["yandex"]["cloud"]['bucket']['image']
        image_items = 0
        document_items = 0
        
        for item in book.get_items():
            item_type = item.get_type()
            
            # Skip unsupported types early
            if item_type in [ITEM_STYLE, ITEM_FONT]:
                continue
            
            if item_type == ITEM_NAVIGATION:
                if item.get_content().strip():
                    outputs.append("<!-- mdformat-toc start --no-anchors -->")
                    continue
            
            # Get and validate content
            content = item.get_content().strip()
            if not content:
                print(f"Empty content in {item.get_name()}")
                continue
            
            # Handle images
            if item_type in [ITEM_IMAGE, ITEM_COVER]:
                continue
                # image_items += 1
                # match item.media_type:
                #     case 'image/jpeg':
                #         ext = "jpeg"
                #     case 'image/png':
                #         ext = "png"
                #     case 'image/svg+xml':
                #         ext = "svg"
                #     case _: raise ValueError(f"Unsupported media type: {item.media_type}")
                # path = os.path.join(clips_dir, f"{doc.md5}-{clips_counter}.{ext}")
                # clips_counter += 1
                # with open(path, "wb") as f:
                #     f.write(content)
                # url = upload_file(path, clips_bucket, os.path.basename(path), s3session, skip_if_exists=True)
                # literal = f'<figure style="text-align: center; margin: 1em 0;" id="{os.path.basename(item.get_name())}"><img alt="" src="{url}" style="max-width: 800px; width: 50%; height: auto;"></figure>'
                # outputs.append(literal)
            
            elif item_type == ITEM_DOCUMENT:
                document_items += 1
                soup = BeautifulSoup(content, 'html.parser').html
                for p in soup.find_all('p'):
                    for i, content in enumerate(p.contents):
                        if isinstance(content, NavigableString):
                            # Replace the content with \n removed
                            p.contents[i].replace_with(content.replace('\n', ' '))
                
                # Remove <a> tag with relative href but keep the text
                for a in soup.find_all('a', href=True):
                    if _is_relative(a['href']):
                        a.unwrap() 
                        
                text_html = str(soup)
                if not text_html.strip():
                    print(f"No HTML after parsing in {item.get_name()}")
                    continue
                
                text_md = md(text_html, bullets='*+-', strong_em_symbol='*', escape_misc=False, heading_styles='atx', table_infer_header=True)
                if text_md.strip():
                    outputs.append(text_md)
                else:
                    print(f"Markdown is empty for {item.get_name()}")
            elif item_type == ITEM_UNKNOWN:
                print("Unknown type found")
            else:
                raise ValueError(f"Unexpected type received: {item_type}")
                
        return "\n\n".join(outputs), (image_items, document_items)

