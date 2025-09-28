from monocorpus_models import Document, Session, SCOPES
from rich import print 
from rich.progress import track
from s3 import download
from utils import read_config, get_in_workdir
from dirs import Dirs
import zipfile
import json
import os
from sqlalchemy import select, text


def upload():
    output_dir = get_in_workdir(Dirs.METADATA)
    limit = 500
    files_without_metadata = set()
    while True:
        with Session() as reader_session, Session() as uploader_session:
            reader_session.query(text("select 1"))
            uploader_session.query(text("select 1"))
            statement = select(Document).where(Document.metadata_json.is_(None) & Document.md5.not_in(files_without_metadata)).limit(limit)
            docs = list(reader_session.query(statement))
            if not docs:
                print("No more documents without metadata found, exiting...")
                break
            
            for doc in track(docs, "Processing documents without metadata..."):
                if not os.path.exists(os.path.join(output_dir, f"{doc.md5}-meta.zip")):
                    print(f"Metadata file for md5={doc.md5} not found, skipping...")
                    files_without_metadata.add(doc.md5)
                    continue
                with zipfile.ZipFile(os.path.join(output_dir, f"{doc.md5}-meta.zip"), 'r') as zf:
                    _json_files = [f for f in zf.namelist() if f.endswith('.json')]
                    if len(_json_files) != 1:
                        raise ValueError(f"Expected exactly one json file in the zip, found {len(_json_files)} in file {doc.md5}-meta.zip: {_json_files}")
                    
                    if not (_content := zf.read(_json_files[0])):
                        print(f"Metadata is empty for md5={doc.md5}, skipping it...")
                        continue
                    
                    doc.metadata_json = json.dumps(json.loads(_content), ensure_ascii=False, indent=None, separators=(',', ':'))
                    print("Uploading metadata for document:", doc.md5)
                    uploader_session.update(doc)