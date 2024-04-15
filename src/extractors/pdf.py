import os.path
import os.path
import re
from collections import Counter

import pdfplumber
from consts import Dirs
from extractors.base import Extractor
from pdfplumber.page import Page, CroppedPage
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
    def extract(self, source_id, path_to_src_file):
        full_file_name = os.path.basename(path_to_src_file)
        file_name, _ = os.path.splitext(full_file_name)
        path_to_txt_file = os.path.join(Dirs.DIRTY.get_real_path(), file_name + ".txt")
        with pdfplumber.open(path_to_src_file) as input, open(path_to_txt_file, 'w', encoding='utf-8') as output:
            output.writelines(f"=====#{source_id}#=====Please do not delete this identification\n")

            mcts = self._find_the_most_popular_font(input.pages)  # most common text size

            for page in track(input.pages, description='Extract text with the most common font size'):
                formatted_text = self._extract_text_from_page(mcts, page)
                if formatted_text:
                    output.write(formatted_text)
        return path_to_txt_file

    def _find_the_most_popular_font(self, pages):
        font_sizes = Counter()  # to find the most popular font size (apprx. core text)

        pages_count = len(pages)
        if pages_count < 10:
            pages = pages[::]
        elif pages_count < 50:
            pages = pages[3:-3]
        else:
            pages = pages[5:-5: len(pages) // 100 + 1]

        for page in track(pages, description='Determining the most common font size in text'):
            for char in page.chars:  # count font_size of each symbol
                font_sizes[char["size"]] += 1
        return font_sizes.most_common(1)[0][0]  # most common text size

    def _extract_text_from_page(self, mcts: float, page: Page):
        bbox = None  # Bounding box is a rectangle area that defines the coordinates of text blocks

        # We enhance bounding box with adding coordinates of chars on the page
        # This way we define coordinates of text blocks with the font size bigger than most common font size
        for char in page.chars:
            if char["size"] < mcts:  # font size is most common of bigger (title, header, etc.)
                continue

            if bbox is None:
                bbox = {k: char[k] for k in LEFT_COORD_KEYS + RIGHT_COORD_KEYS}
            else:
                for key in LEFT_COORD_KEYS:
                    if char[key] < bbox[key]:  # less coords, bigger box
                        bbox[key] = char[key]
                for key in RIGHT_COORD_KEYS:
                    if char[key] > bbox[key]:  # bigger coords, bigger box
                        bbox[key] = char[key]

        if bbox is None:  # found 0 chars with font_size or bigger in this page
            return

        bbox = [bbox[k] for k in LEFT_COORD_KEYS + RIGHT_COORD_KEYS]

        # Crop page with bounding box. This way everything outside the box will be removed, including footers, headers,
        # page numbers, etc.
        cropped_page = page.within_bbox(bbox, strict=True)
        return self._accumulate_text(cropped_page, mcts)

    def _accumulate_text(self, cropped_page: CroppedPage, mcts: float):
        lines = cropped_page.extract_text_lines()  # process each line separately
        formatted_text = ''
        prev_x0 = 0

        # minimal left margin of the text block on the page
        min_x0 = min({line['x0'] for line in lines})
        # average width of the character in the text block
        avg_char_width = sum((line['x1'] - line['x0']) for line in lines) / sum(len(line['text']) for line in lines)

        for line in lines:
            text = line['text']
            x0 = line['x0']

            if len(text) <= 1:  # skip lines containing none or one character
                continue

            # test if endswith hyphen or it look-alike(e.g. тәкер-\nмән)
            # we use regexp to check if hyphen preceded by a non-whitespace character to avoid cases of direct speech
            # example >>> Кояшны бер күрергә тилмерәбез, -\nдип, Аю тирәсендә өтәләнделәр болар.
            is_hyphen = re.match(r".*\S+[-|–|\xad]$", text)

            # Test if the line is a new paragraph
            # It should meet the following conditions:
            # - the left margin is bigger than the minimal left margin plus the width of the average character
            # - the left margin is bigger than the previous line's left margin plus the width of the average character
            is_new_paragraph = x0 > (min_x0 + avg_char_width) and x0 > (prev_x0 + avg_char_width)

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
