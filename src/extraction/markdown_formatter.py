import os
import string
from collections import Counter
import re
from enum import Enum

import typer
from pymupdf import pymupdf, Matrix, TEXT_PRESERVE_LIGATURES, TEXT_PRESERVE_IMAGES, TEXT_PRESERVE_WHITESPACE, \
    TEXT_CID_FOR_UNKNOWN_UNICODE

from consts import Dirs
from file_utils import get_path_in_workdir

non_formatting_chars = string.punctuation + string.whitespace + '–'

# see https://pymupdf.readthedocs.io/en/latest/vars.html#text-extraction-flags
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

class MarkdownFormatter:

    def __init__(self, doc, output_file):
        self.doc = doc
        self.output_file = output_file
        # keep the map of the font sizes to the header levels, the key is the font size, the value is the header level
        self.headers = {}
        # accumulates the various sections of the current page, including texts, images, tables, formulas, etc.
        self.sections = []
        # accumulates the text chunks of the current text block, they will be joined and added as a section
        self.spans = []
        self.image_counter = 0
        self.monospace_in_progress = False
        self.italic_in_progress = False
        self.header_in_progress = False
        self.superscript_in_progress = False
        self.bold_in_progress = False
        self.prev_section_type = None
        # labeled_footnotes in the document, key is the page number, value is the list of labeled footnotes
        self.labeled_footnotes = {}
        # detected superscripts in the document, key is the page number, value is dict with key as the footnote number in
        # the whole document and value in the text of the superscript
        self.found_superscripts = {}
        # counter of labeled_footnotes in the document
        self.footnotes_counter = 0
        self.page = None

        self._determine_header_level(doc)


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

    def _format_span(self, span, is_last_span=False):
        text = span.get('text')
        # if no text, then return None
        if not text:
            return None
        # if the text is only whitespaces or special characters, then return the text as is
        # we close superscript to not add the punctuation to the superscript sign in case superscript is the prev span
        elif text.isspace() or all(c in non_formatting_chars for c in text):
            self._close_superscript()
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

        formatting = "{}"
        # monospace should be the first, otherwise all formatting elements will be inside the monospace block
        if self.monospace_in_progress and not monospace:
            # there is a monospace block in progress but the current span is not monospace, so we need to close the
            # existing monospace block
            self._close_monospace()
        elif not self.monospace_in_progress and monospace:
            self.monospace_in_progress = True
            formatting = f"`{formatting}"

        if self.italic_in_progress and not italic:
            # there is an italic block in progress but the current span is not italic, so we need to close the
            # italic block
            self._close_italic()
        elif not self.italic_in_progress and italic:
            self.italic_in_progress = True
            formatting = f"*{formatting}"

        if self.bold_in_progress and not bold:
            # there is a bold block in progress but the current span is not bold, so we need to close the bold block
            self._close_bold()
        elif not self.bold_in_progress and bold and not header_multiplier:
            # making a header bold does not make sense, because headers are already bold
            self.bold_in_progress = True
            formatting = f"**{formatting}"

        # superscript should be before header
        if self.superscript_in_progress and not superscript:
            # there is a superscript block in progress but the current span is not superscript, so we need to
            # close the superscript block
            self._close_superscript()
        elif not self.superscript_in_progress and superscript:
            self.superscript_in_progress = True
            self.footnotes_counter += 1

            if self.page.number not in self.found_superscripts:
                self.found_superscripts[self.page.number] = {}
            # todo sup text can be anything
            sup_text = text.rstrip(string.punctuation)
            self.found_superscripts[self.page.number][self.footnotes_counter] = sup_text

            formatting = f"[^{self.footnotes_counter}"

        # header should be the last
        if self.header_in_progress and not header_multiplier:
            # there is a header block in progress but the current span is not a header, so we need to close
            # the header
            self._close_header()
        elif not self.header_in_progress and header_multiplier:
            self.header_in_progress = True
            formatting = f"{'#' * header_multiplier} {formatting}"

        # post_processed_text = post_process(text, escape_markdown=not monospace)

        formatting = formatting.format(text)

        return formatting

    def extract_text(self, bbox, keep_line_breaks=False):
        blocks = self.page.get_text("dict", clip=bbox, flags=TEXT_EXTRACTION_FLAGS)['blocks']
        b_len = len(blocks) - 1
        for b_idx, b in enumerate(blocks):
            lines = b.get('lines', [])
            l_len = len(lines) - 1
            for l_idx, li in enumerate(lines):
                spans = li.get('spans', [])
                spans_len = len(spans) - 1
                for s_idx, s in enumerate(li.get('spans', [])):
                    is_last_span = s_idx == spans_len
                    if formatted_span := self._format_span(s, is_last_span):
                        self.spans.append(formatted_span)

                if keep_line_breaks and self.spans and not (b_idx == b_len and l_idx == l_len):
                    # if it is not the last block, then add a line break
                    self.spans[-1] = self.spans[-1].strip()
                    self.spans.append("</br>")

        if self.spans:
            self._close_existing_formatting()
            text_block = re.sub(r'\n+', r'', ''.join(self.spans).strip())
            text_block = re.sub(r'\s+', r' ', text_block)
            text_block = re.sub(r'(</br>)+', r'</br>', text_block)
            self.sections.append((_SectionType.TEXT, text_block))
            self.spans = []
            return text_block

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
                self.spans[-1] = f"{self.spans[-1].rstrip()}]"

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

    def flush(self):
        """
        Flush the sections to the output file
        This method is responsible for proper separation of the sections with new lines
        """
        for ty, section in self.sections:
            prev = self.prev_section_type
            match ty:
                # two new lines before the section
                case _SectionType.IMAGE if prev in (_SectionType.TEXT, _SectionType.IMAGE):
                    section = f"\n\n{section}"
                case _SectionType.TABLE if prev in (_SectionType.TEXT, _SectionType.TABLE, _SectionType.IMAGE):
                    section = f"\n\n{section}"
                case _SectionType.TEXT if prev == _SectionType.IMAGE:
                    section = f"\n\n{section}"
                case _SectionType.TEXT if prev == _SectionType.TEXT:
                    section = f"\n\n{section}"

                # one new line before the section
                case _SectionType.FORMULA | _SectionType.IMAGE:
                    section = f"\n{section}"
                case _SectionType.TABLE if prev == _SectionType.FORMULA:
                    section = f"\n{section}"
                case _SectionType.TEXT if prev in (_SectionType.FORMULA, _SectionType.TABLE):
                    section = f"\n{section}"
            self.output_file.write(section)
            self.prev_section_type = ty
        self.sections = []