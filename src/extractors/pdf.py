import os.path
import os.path
import re

import pdfplumber
from consts import Dirs
from extractors.base import Extractor
from pdfplumber.page import Page
from rich.progress import track

"""
Top left coordinates of bounding box
"""
LEFT_COORD_KEYS = ['x0', 'top']  # less coords, bigger box
"""
Bottom right coordinates of bounding box
"""
RIGHT_COORD_KEYS = ['x1', 'bottom']  # bigger coords, bigger box


class PdfExtractor(Extractor):
    def extract(self, path_to_src_file):
        full_file_name = os.path.basename(path_to_src_file)
        file_name, _ = os.path.splitext(full_file_name)
        path_to_txt_file = os.path.join(Dirs.DIRTY.get_real_path(), file_name + ".txt")
        with pdfplumber.open(path_to_src_file) as input, open(path_to_txt_file, 'w', encoding='utf-8') as output:
            for page in track(input.pages, description=f"Extracting text from PDF file '{file_name}'"):
                formatted_text = self._extract_text_from_page(page)
                if formatted_text:
                    output.write(formatted_text)
        return path_to_txt_file

    def _extract_text_from_page(self, page: Page, new_paragraph_margin=True):
        """
        Extracts text from the page and formats it.

        :param page: page to extract text from
        :param new_paragraph_margin: set True if new paragraph in the document is marked by a margin, set False otherwise
        """
        lines = page.extract_text_lines()  # process each line separately

        if not lines:
            return None

        formatted_text = ''
        prev_x0 = 0

        # minimal left margin of the text block on the page
        min_x0 = min({line['x0'] for line in lines})
        # average width of the character in the text block
        avg_char_width = sum((line['x1'] - line['x0']) for line in lines) / sum(len(line['text']) for line in lines)

        for line in lines:
            text = line['text']
            x0 = line['x0']

            if len(text) <= 1 or text.isdigit():  # skip lines containing none or one character or only digits
                continue

            # test if endswith hyphen or it look-alike(e.g. тәкер-\nмән)
            # we use regexp to check if hyphen preceded by a non-whitespace character to avoid cases of direct speech
            # example >>> Кояшны бер күрергә тилмерәбез, -\nдип, Аю тирәсендә өтәләнделәр болар.
            is_hyphen = re.match(r".*\S+[-|–|\xad]$", text)

            if new_paragraph_margin:
                # Test if the line is a new paragraph
                # It should meet the following conditions:
                # - the left margin is bigger than the minimal left margin plus the width of the average character
                # - the left margin is bigger than the previous line's left margin plus the width of the average character
                is_new_paragraph = x0 > (min_x0 + avg_char_width) and x0 > (prev_x0 + avg_char_width)
            else:
                is_new_paragraph = re.match(r".+[!?.:;] *$", formatted_text)

            if is_new_paragraph:
                # then do not add space before the text but add a new line for the new paragraph
                text = '\n' + text
                formatted_text = formatted_text.rstrip(' ')

            if is_hyphen:
                text = text.rstrip('\xad').rstrip('-').rstrip('–').rstrip(' ')
            else:
                text += ' '

            formatted_text += text

        return formatted_text
