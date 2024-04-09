import os.path

from consts import Dirs
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfparser import PDFParser


def pdf_to_text(path_to_pdf_file):
    """
    Extracts text from the pdf file and saves it to the txt file

    :param path_to_pdf_file: Path to the pdf file
    :return: Path to the txt file with extracted text
    """
    print(f"Extracting text from {path_to_pdf_file}")

    file_name, _ = os.path.splitext(os.path.basename(path_to_pdf_file))
    path_to_txt_path = os.path.join(Dirs.DIRTY.get_real_path(), file_name + ".txt")
    with open(path_to_pdf_file, "rb") as input, open(path_to_txt_path, 'w', encoding='utf-8') as output:
        parser = PDFParser(input)
        doc = PDFDocument(parser)
        rsrcmgr = PDFResourceManager()
        layout_params = LAParams(
            line_overlap=0.5,  # how much 2 chars overlap to be considered as a single word
            char_margin=2.0,  # how close 2 chars must be to each other to be considered as a single word
            line_margin=2,  # how close 2 lines must be to each other to be considered as a single paragraph
            word_margin=0.1,  # how close 2 words must be to each other to be considered as a single line
            boxes_flow=0.0,  # how much a horizontal(-1.0) and vertical(1.0) position of a text matters
            detect_vertical=False,  # ignore vertical text
            all_texts=False  # ignore text in figures
        )
        device = TextConverter(rsrcmgr, output, laparams=layout_params)
        interpreter = PDFPageInterpreter(rsrcmgr, device)
        for page in PDFPage.create_pages(doc):
            interpreter.process_page(page)
    return path_to_txt_path
