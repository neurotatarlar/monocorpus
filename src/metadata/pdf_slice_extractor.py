from utils import  get_in_workdir, load_upstream_metadata
from gemini import gemini_api
from metadata.schema import Book
from dirs import Dirs
from itertools import groupby
import pymupdf
import zipfile
from prompt import DEFINE_META_PROMPT_PDF_HEADER, DEFINE_META_PROMPT_BODY, DEFINE_META_PROMPT_TT_FOOTER, DEFINE_META_PROMPT_CRH_FOOTER
import requests
import json


class FromPdfSliceMetadataExtractor:
    
    
    def __init__(self, doc, config, gemini_client, model, local_doc_path, lang_tag): 
        self.doc = doc
        self.config = config
        self.gemini_client = gemini_client
        self.model = model
        self.local_doc_path = local_doc_path
        self.lang_tag = lang_tag
        
        
    def extract(self):
        # create a slice of first n and last n pages
        slice_file_path = get_in_workdir(Dirs.DOC_SLICES, self.doc.md5, file=f"slice-for-meta")
        slice_page_count, original_doc_page_count = self._prepare_slices(slice_file_path, n=5)
        self.doc.page_count = original_doc_page_count
        
        # prepare prompt
        prompt = self._prepare_prompt(slice_page_count)
        # write prompt to file for debugging
        with open(get_in_workdir(Dirs.PROMPTS, file=f"{self.doc.md5}-meta-prompt.txt"), "w") as f:
            f.write(json.dumps(prompt, ensure_ascii=False, indent=4))
        
        # send to gemini
        files = {slice_file_path: self.doc.mime_type}
        uploaded_files = []
        try:
            response, uploaded_files = gemini_api(client=self.gemini_client, model=self.model, prompt=prompt, files=files, schema=Book, timeout_sec=120)
            
            # validate response
            if not (raw_response := "".join([ch.text for ch in response if ch.text])):
                return None
            else:
                return Book.model_validate_json(raw_response)
        finally:
            for file in uploaded_files:
                try:
                    self.gemini_client.files.delete(name=file.name)
                except Exception as e:
                    print(f"Failed to delete file {file.name}: {e}")

        
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
        prompt.append({'text': DEFINE_META_PROMPT_TT_FOOTER if self.lang_tag == 'tt' else DEFINE_META_PROMPT_CRH_FOOTER})
        if raw_input_metadata := load_upstream_metadata(self.doc.upstream_meta_url, self.doc.md5):
            prompt.append({
                "text": "📌 In addition to the content of the document, you are also provided with external metadata in JSON format. This metadata comes from other sources and should be treated as valid and trustworthy. Consider it alongside the doc content as if it were extracted from the document itself:"
            })
            prompt.append({
                "text": raw_input_metadata
            })
        prompt.append({"text": "Now, extract metadata from the following document"})
        return prompt
    