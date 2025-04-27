from progress import ProgressRenderer


class Context():

    def __init__(self, config, cli_params):
        self.config = config
        self.ya_public_url = cli_params.public_url
        self.cli_params = cli_params

        self.ya_file_name = None
        self.ya_public_key = None 
        self.ya_resource_id = None

        self.progress = ProgressRenderer(self)
        
        self.local_doc_path = None
        # self.local_meta_path = None
        self.local_content_path = None
        self.local_content_path_raw = None
                
        # self.remote_doc_url = None
        # self.remote_meta_url = None
        self.remote_content_url = None
        
        self.md5 = None
        self.gsheet_doc = None
        
        # self.metadata = None
        self.extraction_method = None
        # count of pages in the document, not in the book inside document
        self.doc_page_count = None
        self.tokens = []

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
