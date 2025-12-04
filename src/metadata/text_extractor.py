from utils import decrypt
from prompt import DEFINE_META_PROMPT_NON_PDF_HEADER, DEFINE_META_PROMPT_BODY, DEFINE_META_PROMPT_TT_FOOTER, DEFINE_META_PROMPT_CRH_FOOTER
from utils import get_in_workdir
from dirs import Dirs
from gemini import gemini_api
from metadata.schema import Book
import zipfile
import requests
import json

class FromTextMetadataExtractor:
    
    
    def __init__(self, doc, config, gemini_client, model, lang_tag):
        self.doc = doc
        self.config = config
        self.gemini_client = gemini_client
        self.model = model
        self.lang_tag = lang_tag
    
                
    def extract(self):
        slice = self._load_extracted_content()
        # prepare prompt
        prompt = self._prepare_prompt(slice)
        # write prompt to file for debugging
        with open(get_in_workdir(Dirs.PROMPTS, file=f"{self.doc.md5}-meta-prompt.txt"), "w") as f:
            f.write(json.dumps(prompt, ensure_ascii=False, indent=4))
        response, _ = gemini_api(client=self.gemini_client, model=self.model, prompt=prompt, schema=Book, timeout_sec=120)
        del prompt
        # validate response
        if not (raw_response := "".join([ch.text for ch in response if ch.text])):
            return None
        else:
            return Book.model_validate_json(raw_response)
    
    
    def _load_extracted_content(self, first_N=30_000):
        content_zip = get_in_workdir(Dirs.CONTENT, file=f"{self.doc.md5}.zip")
        content_url = decrypt(self.doc.content_url, self.config) if self.doc.sharing_restricted else self.doc.content_url
        
        with open(content_zip, "wb") as um_zip, requests.get(content_url, stream=True) as resp:
            resp.raise_for_status()
            for chunk in resp.iter_content(chunk_size=512): 
                um_zip.write(chunk)

        content_dir = get_in_workdir(Dirs.CONTENT)
        with zipfile.ZipFile(content_zip, 'r') as enc_zip:
            content_path = enc_zip.extract(f"{self.doc.md5}.md", content_dir)
            
        with open(content_path, "r") as f:
            return f.read(first_N)
        
        
    def _prepare_prompt(self, slice):
        prompt = DEFINE_META_PROMPT_NON_PDF_HEADER.format(n=len(slice))
        prompt = [{'text': prompt}]
        prompt.append({'text': DEFINE_META_PROMPT_BODY})
        prompt.append({'text': DEFINE_META_PROMPT_TT_FOOTER if self.lang_tag == 'tt' else DEFINE_META_PROMPT_CRH_FOOTER})
        prompt.append({"text": "Now, extract metadata from the following extraction from the document"})
        prompt.append({"text": slice})
        return prompt