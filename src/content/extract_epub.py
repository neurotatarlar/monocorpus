from yadisk_client import YaDisk
from utils import read_config, obtain_documents, download_file_locally, get_in_workdir
from monocorpus_models import Document, Session
from ebooklib import epub, ITEM_NAVIGATION, ITEM_DOCUMENT, ITEM_IMAGE, ITEM_STYLE, ITEM_FONT
from bs4 import BeautifulSoup, NavigableString
from markdownify import MarkdownConverter
import mdformat
from dirs import Dirs
from rich import print
import re
from s3 import upload_file, create_session
import os
import zipfile
from urllib.parse import urlparse

class CustomConverter(MarkdownConverter):
    def convert_img(self, el, text, parent_tags):
        pass


def extract_structured_content(cli_params):
    config = read_config()
    predicate = (
        Document.content_url.is_(None) 
        & Document.mime_type.is_('application/epub+zip')
    )
    s3session = create_session(config)
    with YaDisk(config['yandex']['disk']['oauth_token']) as ya_client, Session() as gsheets_session:
        docs = obtain_documents(cli_params, ya_client, predicate, limit=cli_params.limit)
        if not docs:
            print("No documents for processing...")
            return
        
        for doc in docs:
            print(f"Extracting content from file {doc.md5}({doc.file_name})")
            local_doc_path = download_file_locally(ya_client, doc)
            md_content = _extract_from_epub(doc, config, local_doc_path, s3session) 
            formatted_response_md = get_in_workdir(Dirs.CONTENT, file=f"{doc.md5}-formatted.md")
            postprocessed = _postprocess(md_content)
            formatted_content = mdformat.text(
                postprocessed,
                codeformatters=(),
                extensions=["toc", "footnote"],
                options={"wrap": "keep", "number": "keep", "validate": True, "end_of_line": "lf"},
            )
            with open(formatted_response_md, 'w') as f:
                f.write(formatted_content)
                
            content_bucket = config["yandex"]["cloud"]['bucket']['content']
            content_key = f"{doc.md5}-content.zip"
            local_content_path = get_in_workdir(Dirs.CONTENT, file=f"{doc.md5}.zip")
            with zipfile.ZipFile(local_content_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
                zf.write(arcname=f"{doc.md5}.md", filename=formatted_response_md)
            doc.content_url = upload_file(local_content_path, content_bucket, content_key, s3session)

            document_bucket = config["yandex"]["cloud"]['bucket']['document']
            doc_key = os.path.basename(local_doc_path)
            doc.document_url = upload_file(local_doc_path, document_bucket, doc_key, s3session, skip_if_exists=True)
            
            gsheets_session.update(doc)
            
def _postprocess(content):
    content = re.sub(r"^xml version='1\.0' encoding='utf-8'\?\s*", '', content, flags=re.MULTILINE)
    content = re.sub(r"^!\[\]\(.*?\)\s*", '', content, flags=re.MULTILINE)
    content = re.sub(r'^- ?', '— ', content, flags=re.MULTILINE)

    return content
        
def _extract_from_epub(doc, config, local_doc_path, s3session):
    outputs = []
    book = epub.read_epub(local_doc_path)
    clips_counter = 0
    clips_dir = get_in_workdir(Dirs.CLIPS)
    clips_bucket = config["yandex"]["cloud"]['bucket']['image']
    
    # def _convert_image():
    #     match item.media_type:
    #         case 'image/jpeg':
    #             ext = "jpeg"
    #         case 'image/png':
    #             ext = "png"
    #         case 'image/svg+xml':
    #             ext = "svg"
    #         case _: raise ValueError(f"Unsupported media type: {item.media_type}")
    #     path = os.path.join(clips_dir, f"{doc.md5}-{clips_counter}.{ext}")
    #     clips_counter += 1
    #     with open(path, "wb") as f:
    #         f.write(content)
    #     url = upload_file(path, clips_bucket, os.path.basename(path), s3session, skip_if_exists=True)
    #     literal = f'<figure style="text-align: center; margin: 1em 0;" id="{os.path.basename(item.get_name())}"><img alt="" src="{url}" style="max-width: 800px; width: 50%; height: auto;"></figure>'
    #     outputs.append(literal) 
    
    # class CustomConverter(MarkdownConverter):
    #     def convert_img(self, el, text, parent_tags):
    #         _convert_image()
    
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
        if item_type == ITEM_IMAGE:
            match item.media_type:
                case 'image/jpeg':
                    ext = "jpeg"
                case 'image/png':
                    ext = "png"
                case 'image/svg+xml':
                    ext = "svg"
                case _: raise ValueError(f"Unsupported media type: {item.media_type}")
            path = os.path.join(clips_dir, f"{doc.md5}-{clips_counter}.{ext}")
            clips_counter += 1
            with open(path, "wb") as f:
                f.write(content)
            url = upload_file(path, clips_bucket, os.path.basename(path), s3session, skip_if_exists=True)
            literal = f'<figure style="text-align: center; margin: 1em 0;" id="{os.path.basename(item.get_name())}"><img alt="" src="{url}" style="max-width: 800px; width: 50%; height: auto;"></figure>'
            outputs.append(literal)
        
        elif item_type == ITEM_DOCUMENT:
            soup = BeautifulSoup(content, 'html.parser').html
            for p in soup.find_all('p'):
                for i, content in enumerate(p.contents):
                    if isinstance(content, NavigableString):
                        # Replace the content with \n removed
                        p.contents[i].replace_with(content.replace('\n', ''))
            
            # Remove  <a> tag with relative href but keep the text
            for a in soup.find_all('a', href=True):
                if _is_relative(a['href']):
                    a.unwrap() 
                    
            for i in soup.find_all('img'):
                print("image in the document:", i)

            text_html = str(soup)
            if not text_html.strip():
                print(f"No HTML after parsing in {item.get_name()}")
                continue
            
            text_md = md(text_html, bullets='*+-', strong_em_symbol='*', escape_misc=False, heading_styles='atx', table_infer_header=True)
            if text_md.strip():
                outputs.append(text_md)
            else:
                print(f"Markdown is empty for {item.get_name()}")
        else:
            raise ValueError(f"Unexpected type received: {item_type}")
            
    return "\n\n".join(outputs)

def _is_relative(url):
    parsed = urlparse(url)
    return not parsed.scheme and not parsed.netloc