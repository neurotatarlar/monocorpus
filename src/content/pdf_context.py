class Context():

    def __init__(self, doc, local_doc_path):
        self.doc = doc
        self.md5 = doc.md5
        self.local_doc_path = local_doc_path
        self.chunk_paths = []
    
        self.ya_path = None
        self.ya_public_key = None 
        self.ya_resource_id = None
        self.local_content_path = None
        self.formatted_response_md = None
        self.unformatted_response_md = None
        self.remote_doc_url = None
        self.remote_content_url = None
        self.extraction_method = None
        # count of pages in the document, not in the book inside document
        self.doc_page_count = None


    def __enter__(self):
        # self.progress.__enter__()
        return self
    
    def add_chunk_path(self, path):
        if path not in self.chunk_paths[::-1]:
            self.chunk_paths.append(path)

    def __exit__(self, type, value, traceback):
        # self.progress.__exit__(type, value, traceback)
        pass

    def __str__(self):
        return '%s(%s)' % (
            type(self).__name__,
            ', '.join('%s=%s' % item for item in vars(self).items())
        )
        