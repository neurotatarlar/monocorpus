from progress import ProgressRenderer


class Context():

    def __init__(self, config, public_url):
        self.config = config

        self.ya_public_url = public_url
        self.ya_file_name = None
        self.ya_public_key = None 
        self.ya_resource_id = None

        self.progress = ProgressRenderer()
        self.progress._panel.title = f"Processing doc '{public_url}'"
        
        self.local_doc_path = None
        self.local_meta_path = None
        self.local_content_path = None
                
        self.remote_doc_url = None
        self.remote_meta_url = None
        self.remote_content_url = None
        
        self.md5 = None
        self.gsheet_doc = None
  
        self.extracted_metadata = None
        self.extracted_metadata_zip_path = None
        self.extracted_metadata_file_path = None
        self.extraction_method = None
        # count of pages in the document, not in the book inside document
        self.doc_page_count = None

    def __enter__(self):
        self.progress.__enter__()
        return self

    def __exit__(self, type, value, traceback):
        self.progress.__exit__(type, value, traceback)

    def __str__(self):
        return '%s(%s)' % (
            type(self).__name__,
            ', '.join('%s=%s' % item for item in vars(self).items())
        )
