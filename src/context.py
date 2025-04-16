from progress import ProgressRenderer


class Context():

    def __init__(self, path_to_doc, config, doc_hash):
        self.path_to_doc = path_to_doc
        self.config = config
        self.hash = doc_hash
        self.images = {}
        self.progress = ProgressRenderer()

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
