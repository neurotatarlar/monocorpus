from abc import ABC, abstractmethod


class Extractor(ABC):
    """
    Abstract class for extractors
    """

    @abstractmethod
    def extract(self, source_id, path_to_epub_file) -> str:
        pass
