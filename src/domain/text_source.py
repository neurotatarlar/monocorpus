from uuid import UUID, uuid4
from dataclasses import dataclass
import json
from enum import Enum

@dataclass
class TextSource:
    author: str
    title: str
    normalized_name: str

    def __init__(self, author: str, title: str, normalized_name: str):
        self.author = author
        self.title = title
        self.normalized_name = normalized_name
