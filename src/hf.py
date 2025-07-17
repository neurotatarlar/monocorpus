from rich import print 
import os
from utils import read_config, get_in_workdir
from s3 import download
from dirs import Dirs
import pandas as pd
import zipfile
from monocorpus_models import Document, Session
from sqlalchemy import select
from rich import print

def assemble_dataset():
    config = read_config()
    output_dir = get_in_workdir(Dirs.CONTENT)
    rows = []
    with Session() as gsheet_session:
        docs = {doc.md5 : doc for doc in gsheet_session.query(select(Document).where(Document.content_url.is_not(None)))}
        for content_file in download(bucket=config['yandex']['cloud']['bucket']['content'], download_dir=output_dir):
            md5, _ = os.path.splitext(os.path.basename(content_file))
            if not (doc := docs.get(md5)):
                print(f"No matching document with md5 {md5}, skipping it...")
                continue
            
            with zipfile.ZipFile(content_file, 'r') as zf:
                _md_files = [f for f in zf.namelist()]
                if len(_md_files) != 1:
                    raise ValueError(f"Expected exactly one markdown file in the zip, found {len(_md_files)}")
                md_file = _md_files[0]
                content = zf.read(md_file)
                if not content:
                    print(f"Content is empty for document {doc.md5}, skipping it...")
                    continue
                rows.append({
                    "id": md5,
                    "publish_year": str(doc.publish_date),
                    "genre" : doc.genre,
                    "text": content.decode('utf-8')
                })
                
        # Save to CSV (or convert to Hugging Face dataset later)
        df = pd.DataFrame(rows)
        print(len(df))
        df.to_parquet("tatar_structured_content.parquet", index=False)

