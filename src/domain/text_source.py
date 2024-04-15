from dataclasses import dataclass


@dataclass
class TextSource:
    """
    Represents a text source that is being processed.
    """
    author: str
    title: str
    normalized_name: str

    def __init__(self, author: str, title: str, normalized_name: str):
        self.author = author
        self.title = title
        self.normalized_name = normalized_name
