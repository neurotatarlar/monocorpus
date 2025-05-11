from progress import ProgressRenderer


class Context():

    def __init__(self, config, doc, cli_params, gsheets_session):
        self.config = config
        self.doc = doc
        self.cli_params = cli_params
        self.gsheets_session = gsheets_session
        self.tokens = []
        # self.progress = ProgressRenderer(self)

        self.ya_file_name = None
        self.ya_public_key = None 
        self.ya_resource_id = None
        self.local_doc_path = None
        self.local_content_path = None
        self.formatted_response_md = None
        self.unformatted_response_md = None
        self.remote_doc_url = None
        self.remote_content_url = None
        self.md5 = None
        self.extraction_method = None
        # count of pages in the document, not in the book inside document
        self.doc_page_count = None

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
