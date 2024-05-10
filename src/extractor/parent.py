from abc import ABC, abstractmethod


class ExtractionCliArgs:
    def __init__(self, force: bool, count: int, has_paragraph_indent: bool):
        self.force = force
        self.count = count
        self.has_paragraph_indent = has_paragraph_indent

class Extractor(ABC):
    """
    Abstract class for extractors
    """

    @abstractmethod
    def extract(self, source_id, path_to_file, args: ExtractionCliArgs) -> str:
        pass
