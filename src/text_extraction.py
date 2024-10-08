import json
import os
import random
import string
from collections import Counter
from itertools import groupby
from math import sqrt
from enum import Enum

from pymupdf import pymupdf, Matrix, TEXT_PRESERVE_LIGATURES, TEXT_PRESERVE_IMAGES, TEXT_PRESERVE_WHITESPACE, \
    TEXT_CID_FOR_UNKNOWN_UNICODE

import re
from consts import Dirs
from file_utils import get_path_in_workdir

# todo bold inside the word
# todo Horizontal Rule Best Practices instead of asterisks
# todo Starting Unordered List Items With Numbers
# todo download books by md5
# todo remove glyphen
# todo check if regions overlap
# todo define reading order
# todo sort layouts, including knowledge of page structure
# todo whitespace after the punctuations
# todo headers
# todo proper headers
# todo neytralize asterisks and special characters of Markdown
# todo first block can be a continuation of the previous block
# todo one table over 2 pages
# todo image and caption relations
# todo formula support
# todo ignore texts in tables and images
# todo if no text was deteceted, then do OCR
# todo separate title to a separate project
# todo update instruction
# todo draw bounding boxes on the pdf
# todo line wraps
# toto update screenshots for title
#  check annotations from other
#
# special case for poetry
# sort footnotes as well
# tables can have captions too
# save copy of datasets
#  check not all labels are present
# support for emails and urls

omitted_classes = ['page-header', 'page-footer']
TEXT_EXTRACTION_FLAGS = (
    TEXT_PRESERVE_LIGATURES
    &
    ~TEXT_PRESERVE_IMAGES
    &
    TEXT_PRESERVE_WHITESPACE
    &
    TEXT_CID_FOR_UNKNOWN_UNICODE
)


class _SectionType(Enum):
    TEXT = 1
    IMAGE = 2
    TABLE = 3
    FORMULA = 4
    HEADER = 5


class PageArchetype:
    def __init__(self, layouts):
        groups = {}
        for k, g in groupby(layouts, key=lambda x: x['class']):
            if k in omitted_classes:
                continue
            groups[k] = list(g)

        footnote = groups.pop('footnote', [])
        main_body = sorted([item for sublist in groups.values() for item in sublist], key=lambda l: (l['y'], l['x']))
        self.layouts = main_body + footnote

    def __iter__(self):
        return iter(self.layouts)


class MarkdownFormatter:

    def __init__(self, doc):
        self.headers = {}
        # accumulates the various sections of the current page, including texts, images, tables, formulas, etc.
        self.sections = []
        # accumulates the text chunks of the current text block
        self.spans = []
        self.image_counter = 0
        self._determine_header_level(doc)
        self.monospace_in_progress = False
        self.italic_in_progress = False
        self.header_in_progress = False
        self.superscript_in_progress = False
        self.bold_in_progress = False
        self.prev_section_type = None

    def _determine_header_level(self, doc):
        # key is the font size, value is the count of the characters with this font size
        font_sizes = Counter()
        for page in doc.pages():
            for block in page.get_text("dict").get('blocks', []):
                for line in block.get('lines', []):
                    for span in line.get('spans', []):
                        span_text = span.get('text')
                        if not span_text:
                            continue
                        font_sizes[round(span['size'])] += len(span_text)
        mcf = font_sizes.most_common()
        # remove the most popular font size due to it is the font size of the normal text and do not need to be a header
        most_common_font_size = mcf.pop(0)[0]
        mcf = sorted(
            filter(
                lambda x: x > most_common_font_size,
                map(
                    lambda x: x[0],
                    mcf
                )
            ),
            reverse=True
        )
        # take 6 the biggest font sizes as Markdown headers, filter out the fonts bigger than the most common font size
        # because they are not headers
        for idx, size in enumerate(mcf[:6]):
            self.headers[size] = idx + 1

    def _header_for_size(self, size):
        return self.headers.get(round(size)) or 0

    def _format_span(self, span, is_last_span):
        text = span.get('text')
        if not text:
            return None
        elif text.isspace():
            return text

        # check if the last character is a soft hyphen
        is_hyphen = text.endswith('­')
        # if it is the last span in line and not a hyphen, then add a space
        if is_last_span and not is_hyphen:
            text += " "
        elif is_hyphen:
            # remove soft hyphens that are used to split words over lines
            text = text.rstrip('­')

        flags = span['flags']
        size = span['size']

        superscript = bool(flags & 1)
        italic = bool(flags & 2)
        monospace = bool(flags & 8)
        bold = bool(flags & 16)

        header_multiplier = self._header_for_size(size)

        # monospace should be the first, otherwise all formatting elements will be inside the monospace block
        if self.monospace_in_progress and not monospace:
            # there is a monospace block in progress but the current span is not monospace, so we need to close the
            # existing monospace block
            self._close_monospace()
        elif not self.monospace_in_progress and monospace:
            self.monospace_in_progress = True
            text = f"`{text}"

        if self.italic_in_progress and not italic:
            # there is an italic block in progress but the current span is not italic, so we need to close the
            # italic block
            self._close_italic()
        elif not self.italic_in_progress and italic:
            self.italic_in_progress = True
            text = f"*{text}"

        if self.bold_in_progress and not bold:
            # there is a bold block in progress but the current span is not bold, so we need to close the bold block
            self._close_bold()
        elif not self.bold_in_progress and bold and not header_multiplier:
            # making a header bold does not make sense, because headers are already bold
            self.bold_in_progress = True
            text = f"**{text}"

        # superscript should be before header
        if self.superscript_in_progress and not superscript:
            # there is a superscript block in progress but the current span is not superscript, so we need to
            # close the superscript block
            self._close_superscript()
        elif not self.superscript_in_progress and superscript:
            self.superscript_in_progress = True
            text = f"<sup>{text}"

        # header should be the last
        if self.header_in_progress and not header_multiplier:
            # there is a header block in progress but the current span is not a header, so we need to close
            # the header
            self._close_header()
        elif not self.header_in_progress and header_multiplier:
            self.header_in_progress = True
            text = f"{'#' * header_multiplier} {text}"

        return text

    def extract_text(self, bbox, ctxt):
        page = ctxt['page']
        for b in page.get_text("dict", clip=bbox, flags=TEXT_EXTRACTION_FLAGS)['blocks']:
            for li in b.get('lines', []):
                spans = li.get('spans', [])
                spans_len = len(spans) - 1
                for idx, s in enumerate(li.get('spans', [])):
                    is_last_span = idx == spans_len
                    if formatted_span := self._format_span(s, is_last_span):
                        self.spans.append(formatted_span)

        if self.spans:
            self._close_existing_formatting()
            text_block = re.sub(r' +', r' ', ''.join(self.spans).strip())
            self.sections.append((_SectionType.TEXT, text_block))
            self.spans = []

    def _close_header(self):
        self.header_in_progress = False

    def _close_monospace(self):
        if self.monospace_in_progress:
            self.monospace_in_progress = False
            if self.spans:
                self.spans[-1] = f"{self.spans[-1].rstrip()}`"

    def _close_italic(self):
        if self.italic_in_progress:
            self.italic_in_progress = False
            if self.spans:
                self.spans[-1] = f"{self.spans[-1].rstrip()}*"

    def _close_bold(self):
        if self.bold_in_progress:
            self.bold_in_progress = False
            if self.spans:
                self.spans[-1] = f"{self.spans[-1].rstrip()}**"

    def _close_superscript(self):
        if self.superscript_in_progress:
            self.superscript_in_progress = False
            if self.spans:
                self.spans[-1] = f"{self.spans[-1].rstrip()}</sup>"

    def _close_existing_formatting(self):
        """
        Close all formatting blocks if they are in progress
        Order of closing is important
        """
        self._close_monospace()
        self._close_italic()
        self._close_bold()
        self._close_superscript()
        self._close_header()

    def extract_picture(self, bbox, ctxt):
        page = ctxt['page']
        self.image_counter += 1
        file_name = f"{ctxt['md5']}-{self.image_counter}.png"
        path_to_image = os.path.join(get_path_in_workdir(Dirs.ARTIFACTS), file_name)
        page.get_pixmap(clip=bbox, matrix=Matrix(1, 1), dpi=100).save(path_to_image, "png")
        self.sections.append((_SectionType.IMAGE, f"![image{self.image_counter}](./{file_name})"))

    def flush(self, output_file):
        # headers, text blocks, tables, images, formulas
        # for text blocks:
        #  - two line breaks after previous text line
        #  - one line break after the header, formulas, tables, images
        # for tables two new lines after text, another table, image, but one line after the headers and formulas
        # for formulas, images, header one new line before the section

        """
          Extract text from the specified bounding box on the page

          If block is the first, then it can be a continuation of the previous block
          To check it, we need to check:
          - there is any previous text block. If not, then it is a new block
          - if the last text block on the previous page ends with a hyphen, then it is a continuation
          - if the new block starts with long dash, then it is a new block
          - if the new block not starts with a long dash and consists of one line, then it is a continuation
          - if the new block has more than one line, and first line has indent, then it is a new block
          """
        first_text_block_found = False
        for ty, section in self.sections:
            prev = self.prev_section_type
            match ty:
                case _SectionType.FORMULA | _SectionType.IMAGE:
                    section = f"\n{section}"
                case _SectionType.TABLE if prev in (_SectionType.TEXT, _SectionType.TABLE, _SectionType.IMAGE):
                    section = f"\n\n{section}"
                case _SectionType.TABLE if prev == _SectionType.FORMULA:
                    section = f"\n{section}"
                case _SectionType.TEXT if prev in (_SectionType.FORMULA, _SectionType.IMAGE, _SectionType.TABLE):
                    section = f"\n{section}"
                case _SectionType.TEXT if prev == _SectionType.TEXT:
                    # if this is the first text block on the page, then it can be a continuation of the previous block
                    # if not first_text_block_found:
                    #     first_text_block_found = True
                    #     # very common for dialogues
                    #     if section.startswith('—') or section.startswith('–'):
                    #         pass
                    #     else:
                    #         section = f"\n\n{section}"
                    # else:
                    section = f"\n\n{section}"
            output_file.write(section)
            self.prev_section_type = ty
        self.sections = []


def extract_content(md5, path_to_doc, path_to_la):
    """
    Extract text from the files in the entry point folder
    """
    print(f"Extracting text from the document with md5 `{md5}`...")
    result_md = os.path.join(get_path_in_workdir(Dirs.ARTIFACTS), f"{md5}.md")
    with open(path_to_la, 'rb') as f:
        annotations = json.load(f)

    context = {'md5': md5}

    with pymupdf.open(path_to_doc) as doc, open(result_md, 'w') as result_md:
        f = MarkdownFormatter(doc)
        for page in list(doc.pages())[:]:
            if not (page_layouts := annotations.get(str(page.number))):
                raise Exception(f"Annotations for the page `{page.number}` not found")

            context['page'] = page
            width = page.rect.width / 100
            height = page.rect.height / 100
            for idx, anno in enumerate(PageArchetype(page_layouts)):
                x1 = anno['x'] * width
                y1 = anno['y'] * height
                x2 = x1 + anno['width'] * width
                y2 = y1 + anno['height'] * height
                bbox = [x1, y1, x2, y2]

                match anno['class']:
                    case 'picture':
                        f.extract_picture(bbox, context)
                    case 'table':
                        raise NotImplementedError("Table extraction is not implemented yet")
                    case 'formula':
                        raise NotImplementedError("Formula extraction is not implemented yet")
                    case 'poetry':
                        pass
                        # raise NotImplementedError("Poetry extraction is not implemented yet")
                    case 'page-header' | 'page-footer':
                        # we must not be here because we already filtered out these classes
                        continue
                    case _:
                        f.extract_text(bbox, context)

            # flush the page to the file
            f.flush(result_md)
