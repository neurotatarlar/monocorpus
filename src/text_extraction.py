import json
import os
import re

import typer
from pymupdf import pymupdf

from consts import Dirs
from extraction.heuristic_archetype import HeuristicArchetype
from extraction.markdown_formatter import MarkdownFormatter, TEXT_EXTRACTION_FLAGS, _SectionType
from file_utils import get_path_in_workdir

# todo case there first block starts with a title letter
# todo post processing
# todo bold inside the word
# todo Horizontal Rule Best Practices instead of asterisks
# todo Starting Unordered List Items With Numbers
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
# sort labeled_footnotes as well
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
        f = MarkdownFormatter(doc, r)
        for page in doc.pages(start=pages_slice.start, stop=pages_slice.stop, step=pages_slice.step):
            if not (page_layouts := annotations.get(str(page.number))):
                print(f"Page {page.number} has no layout annotations")
                continue

            width = page.rect.width / 100
            height = page.rect.height / 100
            a = HeuristicArchetype(page_layouts['results'])
            f.labeled_footnotes[page.number] = a.footnote
            f.page = page
            for idx, anno in enumerate(a):
                bbox = _calculate_bbox(anno, width, height)
                f.block = (idx, bbox)
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
                        f.extract_text(keep_line_breaks=True)
                    case 'text' | 'title' | 'section-header' | 'list-item':
                        f.extract_text()
                    case 'footnote':
                        # will be processed later after all pages are processed
                        pass
                    case _:
                        print(f"Unexpected class: {anno['class']}")
                        raise typer.Abort()
                # draw bounding boxes on page for visual control
                page.draw_rect(bbox, color=(0, 1, 0), width=1)

            # draw omitted layouts
            for omitted in a.omitted_layouts:
                bbox = _calculate_bbox(omitted, width, height)
                page.draw_rect(bbox, color=(1, 0, 0), width=1)

            # flush the page to the file
            f.flush()

        # save the document with bounding boxes
        path_to_plotted_doc = os.path.join(get_path_in_workdir(Dirs.DOCS_PLOT), f"{md5}.pdf")
        with open(path_to_plotted_doc, 'wb') as plotted_doc:
            doc.save(plotted_doc)

        process_footnotes(f)


def process_footnotes(f):
    """
    Flush the footnotes to the output file
    """
    if not f.labeled_footnotes:
        print("No footnotes was labeled, skipping footnotes extraction")
        return

    for page in f.doc.pages():
        page_number = page.number
        # labeled footnotes for the current page
        lfp = f.labeled_footnotes.get(page_number)
        # detected superscripts for the current page
        dsp = f.found_superscripts.get(page_number)
        if not (lfp and dsp):
            # no footnotes on the page
            continue
        elif lfp and not dsp:
            print(f"Page {page_number} has labeled footnotes but no detected superscripts")
            raise typer.Abort()
        elif not lfp and dsp:
            print(f"Page {page_number} has detected superscripts but no labeled footnotes")
            raise typer.Abort()
        elif len(lfp) != len(dsp):
            print(f"Page {page_number} has different number of labeled footnotes and detected superscripts")

        width = page.rect.width / 100
        height = page.rect.height / 100
        lfp = sorted(lfp, key=lambda x: x['y'])
        dsp = sorted(dsp.items(), key=lambda x: x[0])
        for labeled_footnote, (counter, superscript_text) in zip(lfp, dsp):
            bbox = _calculate_bbox(labeled_footnote, width, height)
            dirty_text = page.get_text("text", clip=bbox, flags=TEXT_EXTRACTION_FLAGS).lstrip()

            dirty_text = re.sub(r'\n+', r'', dirty_text)
            dirty_text = re.sub(r'\s+', r' ', dirty_text)
            pattern = "^" + re.escape(superscript_text) + r"\D+"
            if re.match(pattern, dirty_text):
                # remove superscript from the text
                dirty_text = dirty_text[len(superscript_text):].strip()
                f.sections.append((None, _SectionType.FOOTNOTE, f"[^{counter}]: {dirty_text}"))
            else:
                print(
                    f"page:{page_number} footnote:{counter} superscript:`{superscript_text}` not found in the text: {dirty_text}")

    f.flush()


def _calculate_bbox(s, p_width, p_height):
    x1 = s['x'] * p_width
    y1 = s['y'] * p_height
    x2 = x1 + s['width'] * p_width
    y2 = y1 + s['height'] * p_height
    return [x1, y1, x2, y2]
