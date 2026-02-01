"""Shared context object for PDF extraction and postprocessing."""


class Context():
    """Holds per-document state during PDF extraction."""

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
        """Enable use as a context manager."""
        # self.progress.__enter__()
        return self
    
    def add_chunk_path(self, path):
        """Track a chunk output path if it is new."""
        if path not in self.chunk_paths[::-1]:
            self.chunk_paths.append(path)

    def __exit__(self, type, value, traceback):
        """Context manager exit hook (currently no-op)."""
        # self.progress.__exit__(type, value, traceback)
        pass

    def __str__(self):
        """Return a debug-friendly string representation."""
        return '%s(%s)' % (
            type(self).__name__,
            ', '.join('%s=%s' % item for item in vars(self).items())
        )
        
