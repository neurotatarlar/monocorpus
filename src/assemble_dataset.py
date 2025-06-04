from huggingface_hub import hf_hub_download
from rich import print 
import boto3
import os
from utils import read_config, get_in_workdir
from s3 import download
from dirs import Dirs
import pandas as pd
import zipfile

def assemble_dataset():
    config = read_config()
    output_dir = get_in_workdir(Dirs.CONTENT)
    rows = []
    for content_file in download(bucket=config['yandex']['cloud']['bucket']['content'], download_dir=output_dir):
        # print(f"Processing file {content_file}")
        with zipfile.ZipFile(content_file, 'r') as zf:
            _md_files = [f for f in zf.namelist()]
            if len(_md_files) != 1:
                raise ValueError(f"Expected exactly one markdown file in the zip, found {len(_md_files)}")
            md_file = _md_files[0]
            content = zf.read(md_file)
            rows.append({"text": content})
            
    # Save to CSV (or convert to Hugging Face dataset later)
    df = pd.DataFrame(rows)
    df.to_csv("tatar_books_dataset.csv", index=False)

