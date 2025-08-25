from utils import  get_in_workdir
from gemini import gemini_api
from metadata.schema import Book
import zipfile
import requests
from dirs import Dirs
import os
from itertools import groupby
import pymupdf
import zipfile
from prompt import DEFINE_META_PROMPT_PDF_HEADER, DEFINE_META_PROMPT_BODY
import requests
import json


class FromPdfSliceMetadataExtractor:
    
    
    def __init__(self, doc, config, gemini_client, s3lient, model, local_doc_path): 
        self.doc = doc
        self.config = config
        self.gemini_client = gemini_client
        self.s3lient = s3lient
        self.model = model
        self.local_doc_path = local_doc_path
        
        
    def extract(self):
        # create a slice of first n and last n pages
        slice_file_path = get_in_workdir(Dirs.DOC_SLICES, self.doc.md5, file=f"slice-for-meta")
        slice_page_count, original_doc_page_count = self._prepare_slices(slice_file_path, n=3)
        self.doc.page_count = original_doc_page_count
        
        # prepare prompt
        prompt = self._prepare_prompt(slice_page_count)
        
        # send to gemini
        files = {slice_file_path: self.doc.mime_type}
        response = gemini_api(client=self.gemini_client, model=self.model, prompt=prompt, files=files, schema=Book, timeout_sec=180)
        del prompt
        
        # validate response
        if not (raw_response := "".join([ch.text for ch in response if ch.text])):
            return None
        else:
            return Book.model_validate_json(raw_response)

        
    def _prepare_slices(self, dest_path, n):
        """
        Prepare aux PDF doc with slices of pages of the original document for metadata extraction.
        :param pdf_doc: The PDF document to slice.
        :param n: Number of pages to include from the start.
        :return: The number of pages in the new document and the original document.
        """
        def __ranges(_i):
            for _, _b in groupby(enumerate(_i), lambda pair: pair[1] - pair[0]):
                _b = list(_b)
                yield _b[0][1], _b[-1][1]
        
        
        with pymupdf.open(self.local_doc_path) as pdf_doc, pymupdf.open() as doc_slice:
            pages = list(range(0, pdf_doc.page_count))
            pages = set(pages[:n] + pages[-n:])
            for start, end in list(__ranges(pages)):
                doc_slice.insert_pdf(pdf_doc, from_page=start, to_page=end)
            doc_slice.save(dest_path)
            return doc_slice.page_count, pdf_doc.page_count
        
        
    def _prepare_prompt(self, slice_page_count):
        prompt = DEFINE_META_PROMPT_PDF_HEADER.format(n=int(slice_page_count / 2),)
        prompt = [{'text': prompt}]
        prompt.append({'text': DEFINE_META_PROMPT_BODY})
        if raw_input_metadata := self._load_upstream_metadata():
            prompt.append({
                "text": "📌 In addition to the content of the document, you are also provided with external metadata in JSON format. This metadata comes from other sources and should be treated as valid and trustworthy. Consider it alongside the doc content as if it were extracted from the document itself:"
            })
            prompt.append({
                "text": raw_input_metadata
            })
        prompt.append({"text": "Now, extract metadata from the following document"})
        return prompt
    
    
    def _load_upstream_metadata(self):
        if not (upstream_metadata_url := self.doc.upstream_metadata_url):
            return None
        upstream_metadata_zip = get_in_workdir(Dirs.UPSTREAM_METADATA, file=f"{self.doc.md5}.zip")
        with open(upstream_metadata_zip, "wb") as um_zip, requests.get(upstream_metadata_url, stream=True) as resp:
            resp.raise_for_status()
            for chunk in resp.iter_content(chunk_size=8192): 
                um_zip.write(chunk)
                
        upstream_metadata_unzip = get_in_workdir(Dirs.UPSTREAM_METADATA, self.doc.md5)
        with zipfile.ZipFile(upstream_metadata_zip, 'r') as enc_zip:
            enc_zip.extractall(upstream_metadata_unzip)
            
        with open(os.path.join(upstream_metadata_unzip, "metadata.json"), "r") as raw_meta:
            _meta = json.load(raw_meta)
            _meta.pop("available_pages", None)
            _meta.pop("doc_card_url", None)
            _meta.pop("download_code", None)
            _meta.pop("doc_url", None)
            _meta.pop("access", None)
            _meta.pop("lang", None)
            return json.dumps(_meta, ensure_ascii=False)