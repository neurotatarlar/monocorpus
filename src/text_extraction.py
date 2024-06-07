import json
import os
from collections import Counter

from pymupdf import pymupdf

from consts import Dirs
from file_utils import get_path_in_workdir


class Formatter:
    pass


# todo detect multiple bolds sequences
# todo line wrap between pages
# todo attention to the first and last layouts on a page
# todo write footnotes
def extract(path_to_doc, path_to_la):
    """
    Extract text from the files in the entry point folder
    """
    md5 = path_to_doc.split("/")[-1].split(".")[0]
    output_path = os.path.join(get_path_in_workdir(Dirs.ARTIFACTS), f"{md5}.md")
    with open(path_to_la, 'rb') as laf:
        la = json.load(laf)
    with pymupdf.open(path_to_doc) as doc, open(output_path, 'w') as output:
        hsd = HeaderSizeDefiner(doc)
        for page in doc.pages():
            res = process_page(page, la, hsd)
            output.write(res)


def analyze_font_sizes(doc):
    """
    Analyze font sizes in the document
    """


def process_page(page, la, hsd):
    page_text = []
    page_no = str(page.number)
    page_layout = la.get(page_no)

    if not page_layout:
        raise Exception(f"Page `{page_no}` not found in layout analysis")
    page_layout = order_layouts(page_layout)
    body, footnotes = page_layout
    for l in body:
        bbox = l['bbox']
        cls = l['layout'][0]['class']
        match cls:
            case 'text' | 'section-header' | 'title':
                formatted_text = extract_text(page, bbox, hsd)
                page_text.append(formatted_text)
            case 'page-header' | 'page-footer':
                raise Exception(f"Unexpected layout type `{cls}`, it should be removed during layout ordering")
            case _:
                # raise Exception(f"Unsupported layout type `{cls}`")
                pass
    return "".join(page_text)


def extract_text(page, bbox, hsd):
    result = []
    for b in page.get_text("dict", clip=bbox).get('blocks', []):
        for l in b.get('lines', []):
            for s in l.get('spans', []):
                text = s.get('text')
                if not text:
                    continue
                text = text.strip()
                flags = s['flags']
                superscript = bool(flags & 1)
                italic = bool(flags & 2)
                monospace = bool(flags & 8)
                bold = bool(flags & 16)
                if monospace:
                    text = f"`{text}`"
                asterisks_count = (1 if italic else 0) + (2 if bold else 0)
                text = f"{'*' * asterisks_count}{text}{'*' * asterisks_count}"
                if superscript:
                    text = f"[^{text}]"
                else:
                    text = hsd.header_for_size(s['size'], text)
                result.append(text)
    return " ".join(result) + "\n\n"


def order_layouts(page_layouts):
    def sort(l):
        return sorted(l, key=lambda x: (x['bbox'][1], x['bbox'][0]))

    def leave_most_confident_layout(l):
        return list(map(lambda x: {'bbox': x['bbox'], 'layout': [x['layout'][0]]}, l))

    grouped_layouts = {}
    for l in page_layouts:
        key = l['layout'][0]['class']
        if key in grouped_layouts:
            grouped_layouts[key].append(l)
        else:
            grouped_layouts[key] = [l]

    # skip page headers and footers
    _ = grouped_layouts.pop('page-header', [])
    _ = grouped_layouts.pop('page-footer', [])

    footnotes = leave_most_confident_layout(sort(grouped_layouts.pop('footnote', [])))
    body = leave_most_confident_layout(sort([item for sublist in grouped_layouts.values() for item in sublist]))
    return body, footnotes


class HeaderSizeDefiner:
    def __init__(self, doc):
        font_sizes = Counter()
        for page in doc.pages():
            for block in page.get_text("dict").get('blocks', []):
                for line in block.get('lines', []):
                    for span in line.get('spans', []):
                        span_text = span.get('text')
                        if not span_text:
                            continue
                        font_size = round(span['size'])
                        font_sizes[font_size] += len(span_text)
        mcf = font_sizes.most_common()
        print(mcf)
        # remove the most popular font size due to it is the font size of the normal text and do not need to be a header
        mcf.pop(0)
        mcf = sorted(map(lambda x: x[0], mcf), reverse=True)
        self.headers = {}
        for idx, size in enumerate(mcf[:6]):  # take 6 the biggest font sizes as Markdown headers
            self.headers[size] = idx + 1

    def header_for_size(self, size, text):
        size = round(size)
        if size in self.headers:
            return f"{'#' * self.headers[size]} {text}"
        return text
