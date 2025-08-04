from dataclasses import dataclass
from rich.console import RenderableType
from typing import List

@dataclass
class Message:
    id: str
    content: List[RenderableType]
    complete: bool
    
class Context():

    def __init__(self, config, doc, cli_params, log_queue):
        self.config = config
        self.doc = doc
        self.md5 = doc.md5
        self.cli_params = cli_params
        self.chunk_paths = []
        self.log_queue = log_queue
    
        self.ya_file_name = None
        self.ya_public_key = None 
        self.ya_resource_id = None
        self.local_doc_path = None
        self.local_content_path = None
        self.formatted_response_md = None
        self.unformatted_response_md = None
        self.remote_doc_url = None
        self.remote_content_url = None
        self.extraction_method = None
        # count of pages in the document, not in the book inside document
        self.doc_page_count = None
        self.unmatched_images = 0
        self.total_images = 0

    def __enter__(self):
        # self.progress.__enter__()
        return self

    def __exit__(self, type, value, traceback):
        # self.progress.__exit__(type, value, traceback)
        pass

    def __str__(self):
        return '%s(%s)' % (
            type(self).__name__,
            ', '.join('%s=%s' % item for item in vars(self).items())
        )
        
    def log(self, content, complete=False):
        msg = Message(
            id=self.md5,
            content=content,
            complete=complete
        )
        self.log_queue.put(msg)
