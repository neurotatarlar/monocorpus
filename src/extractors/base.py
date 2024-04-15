import os.path
import os.path
from abc import ABC, abstractmethod

import ebooklib
from bs4 import BeautifulSoup
from ebooklib import epub
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.pdfinterp import resolve1
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfparser import PDFParser
from rich.progress import track

from consts import Dirs


class Extractor(ABC):
    """
    Abstract class for extractors
    """

    @abstractmethod
    def extract(self, source_id, path_to_epub_file) -> str:
        pass