import os.path
import os.path
import re
from collections import Counter

import pdfplumber
from pdfplumber.page import Page
from rich.progress import track

from consts import Dirs

"""
Top left coordinates of bounding box
"""
LEFT_COORD_KEYS = ['x0', 'top']  # less coords, bigger box
"""
Bottom right coordinates of bounding box
"""
RIGHT_COORD_KEYS = ['x1', 'bottom']  # bigger coords, bigger box


class PdfExtractor:
    def extract(self, path_to_src_file, output_path=Dirs.DIRTY.get_real_path()):
        full_file_name = os.path.basename(path_to_src_file)
        file_name, _ = os.path.splitext(full_file_name)
        path_to_txt_file = os.path.join(output_path, file_name + ".md")
        import pymupdf

        prev_span_bbox = [0, 0, 0, 0]
        result = ''
        with pymupdf.open(path_to_src_file) as document:
            for page in list(document.pages())[4:5]:
                for bidx, b in enumerate(page.get_text("dict", sort=True).get('blocks', [])):
                    for lidx, l in enumerate(b.get('lines', [])):
                        for s in l.get('spans', []):
                            print(s)
                            text = s['text']
                            if not text:
                                continue
                            flags = s['flags']
                            superscript = bool(flags & 1)
                            italic = bool(flags & 2)
                            monospace = bool(flags & 8)
                            bold = bool(flags & 16)
                            if monospace:
                                text = f"`{text}`"
                            asterisks_count = (1 if italic else 0) + (2 if bold else 0)
                            text = f"{'*' * asterisks_count}{text}{'*' * asterisks_count}"

                            cur_span_bbox = tuple(map(lambda x: round(x), s['bbox']))
                            print(f"{cur_span_bbox}:{s['text']}")
                            if superscript:
                                text = f"<sup>{text}</sup>"
                            else:
                                if cur_span_bbox[1] > prev_span_bbox[1] and cur_span_bbox[3] > prev_span_bbox[3]:
                                    text = ('\n' if result else '') + text
                                prev_span_bbox = cur_span_bbox
                            result += text

        with open(path_to_txt_file, 'w', encoding='utf-8') as output:
            output.write(result)
        return path_to_txt_file

    def _extract_text_from_page(self, page: Page, mcts, total_pages: int, page_has_paragraph_indent=True,
                                remove_header_footer=False):
        """
        Extracts text from the page and formats it.

        :param page: page to extract text from
        :param mcts: most common text size
        :param page_has_paragraph_indent: set True if new paragraph in the document is marked by a margin, set False otherwise
        """
        if remove_header_footer:
            page = _crop_page(mcts, page)

        lines = page.extract_text_lines()  # process each line separately

        if not lines:
            return None

        if remove_header_footer and len(lines) > 2:
            if page.page_number == 1:
                # keep the first line of the first page
                lines = lines[:-1]
            elif page.page_number == total_pages:
                # keep the last line of the last page
                lines = lines[1:]
            else:
                # remove the first and the last lines of the page otherwise
                lines = lines[1:-1]

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

            if page_has_paragraph_indent:
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


def _find_the_most_popular_font(pages):
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
