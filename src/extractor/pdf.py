import os.path
import os.path
import re
from collections import Counter

import pdfplumber
from consts import Dirs
from extractor.parent import Extractor
from pdfplumber.page import Page
from rich.progress import track

from extractor.parent import ExtractionCliArgs

"""
Top left coordinates of bounding box
"""
LEFT_COORD_KEYS = ['x0', 'top']  # less coords, bigger box
"""
Bottom right coordinates of bounding box
"""
RIGHT_COORD_KEYS = ['x1', 'bottom']  # bigger coords, bigger box


class PdfExtractor(Extractor):
    @staticmethod
    def extract(path_to_src_file, args: ExtractionCliArgs):
        full_file_name = os.path.basename(path_to_src_file)
        file_name, _ = os.path.splitext(full_file_name)
        path_to_txt_file = os.path.join(Dirs.DIRTY.get_real_path(), file_name + ".txt")
        with pdfplumber.open(path_to_src_file) as input, open(path_to_txt_file, 'w', encoding='utf-8') as output:
            all_pages = input.pages
            mc_f_size, mc_f = _analyze_book(all_pages)

            # for page in track(all_pages, description=f"Extracting text from PDF file '{file_name}'"):
            #     formatted_text = _extract_text_from_page(page, mc_f_size, args.has_paragraph_indent)
            #
            #     if formatted_text:
            #         output.write(formatted_text)
            #     return path_to_txt_file
        return path_to_txt_file

def _extract_text_from_page(page, mc_f_size, has_paragraph_indent):
    """
    Extracts text from the page and formats it.

    :param page: page to extract text from
    :param mcts: most common text size
    :param page_has_paragraph_indent: set True if new paragraph in the document is marked by a margin, set False otherwise
    """
    page = _crop_page(mc_f_size, page)

    lines = page.extract_text_lines()  # process each line separately
    page.extract_text_lines()

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

        if has_paragraph_indent:
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


def _analyze_book(pages):
    font_sizes = Counter()  # to find the most popular font size
    fonts = Counter()  # to find the most popular font family

    pages_count = len(pages)

    if pages_count < 10:
        pages_slice = pages[::]
    elif pages_count < 50:
        pages_slice = pages[3:-3]
    else:
        pages_slice = pages[5:-5: len(pages) // 100 + 1]

    for page in track(pages_slice, description='Determining the most common fonts and font size'):
        for char in page.chars:  # count font_size of each symbol
            font_sizes[char["size"]] += 1
            fonts[char["fontname"]] += 1

    mcfs = font_sizes.most_common(1)[0][0]  # most common font size
    mcf = fonts.most_common(1)[0][0]  # most common font family

    headers = []
    for page in track(pages, description='Determining the most common font size in text'):
        first_line = page.extract_text_lines()[0]

        font_size = Counter()
        font = Counter()
        for char in first_line['chars']:
            font_size[char["size"]] += 1
            font[char["fontname"]] += 1

        probability = 0.0
        if font_size.most_common(1)[0][0] != mcfs:
            probability += 0.5
        if font.most_common(1)[0][0] != mcf:
            probability += 0.5
        if probability > 0.5:
            headers.append(page)

    return mcfs, mcf


def _crop_page(mcts: float, page: Page):
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
        return page

    bbox = [bbox[k] for k in LEFT_COORD_KEYS + RIGHT_COORD_KEYS]

    # Crop page with bounding box. This way everything outside the box will be removed, including footers, headers,
    # page numbers, etc.
    return page.within_bbox(bbox, strict=True)
