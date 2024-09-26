import json
import os
import re
import string
from collections import Counter
from enum import Enum
from itertools import groupby

import typer
from numpy.core.defchararray import rindex
from pymupdf import pymupdf

from consts import Dirs
from file_utils import get_path_in_workdir
from post_processor import post_process
from extraction.markdown_formatter import MarkdownFormatter, _SectionType
from extraction.heuristic_archetype import HeuristicArchetype

# todo post processing
# todo bold inside the word
# todo Horizontal Rule Best Practices instead of asterisks
# todo Starting Unordered List Items With Numbers
# todo download books by md5
# todo remove glyphen
# todo define reading order
# todo sort layouts, including knowledge of page structure
# todo whitespace after the punctuations
# todo headers
# todo neytralize asterisks and special characters of Markdown
# todo first block can be a continuation of the previous block
# todo one table over 2 pages_slice
# todo image and caption relations
# todo formula support
# todo ignore texts in tables and images
# todo if no text was deteceted, then do OCR
# todo update instruction
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

# . URL: http://www. erzan.ru.
# multiple breaks
# ** А.Х.Садекова, ** филология

pymupdf.TOOLS.set_small_glyph_heights(True)

def extract_content(doc, path_to_doc, path_to_la, pages_slice):
    """
    Extract text from the files in the entry point folder
    """
    md5 = doc.md5
    print(f"Extracting text from the document with md5 `{md5}`...")
    extracted_content = os.path.join(get_path_in_workdir(Dirs.ARTIFACTS), f"{md5}.md")
    with open(path_to_la, 'rb') as f:
        annotations = json.load(f)

    with pymupdf.open(path_to_doc) as doc, open(extracted_content, 'w') as r:
        f = MarkdownFormatter(doc)
        footnotes = {}
        for page in doc.pages(start=pages_slice.start, stop=pages_slice.stop, step=pages_slice.step):
            if not (page_layouts := annotations.get(str(page.number))):
                print(f"Page {page.number} has no layout annotations")
                continue

            width = page.rect.width / 100
            height = page.rect.height / 100
            context = {'page': page}

            a = HeuristicArchetype(page_layouts['results'])
            footnotes[page.number] = a.footnote
            for idx, anno in enumerate(a):
                bbox = _calculate_bbox(anno, width, height)

                match anno['class']:
                    case 'picture':
                        print("Skipping picture extraction")
                        pass
                    case 'table':
                        print("Skipping table extraction")
                        pass
                    case 'formula':
                        print("Skipping formula extraction")
                        pass
                    case 'poetry':
                        f.extract_text(bbox, context, keep_line_breaks=True)
                    case 'text' | 'title' | 'section-header' | 'list-item':
                        f.extract_text(bbox, context)
                    case 'footnote':
                        pass
                    case _:
                        print(f"Unexpected class: {anno['class']}")
                        raise typer.Abort()
                # draw bounding boxes on page for visual control
                page.draw_rect(bbox, color=(0, 1, 0), width=1)

            # flush the page to the file
            f.flush(r)
            # draw omitted layouts
            for omitted in a.omitted_layouts:
                bbox = _calculate_bbox(omitted, width, height)
                page.draw_rect(bbox, color=(1, 0, 0), width=1)

        for c in range(f.footnotes_counter):
            f.sections.append((_SectionType.TEXT, f"[^{c + 1}]: footnote {c + 1}"))
        f.flush(r)

        # save the document with bounding boxes
        path_to_plotted_doc = os.path.join(get_path_in_workdir(Dirs.DOCS_PLOT), f"{md5}.pdf")
        with open(path_to_plotted_doc, 'wb') as f:
            doc.save(f)


def _calculate_bbox(s, p_width, p_height):
    x1 = s['x'] * p_width
    y1 = s['y'] * p_height
    x2 = x1 + s['width'] * p_width
    y2 = y1 + s['height'] * p_height
    return [x1, y1, x2, y2]