import os
import unittest
from pdf import PdfExtractor
import difflib

class RunTests(unittest.TestCase):

    @classmethod
    def setUp(cls):
        resources = os.path.join(os.path.dirname(__file__), "resources")
        all_files = set(os.listdir(resources))

        pdfs = [os.path.join(resources, file) for file in all_files if file.endswith(".pdf")]
        txts = [os.path.join(resources, file) for file in all_files if file.endswith(".txt")]

        pdf_pairs = []
        for pdf in pdfs:
            pdf_name = os.path.splitext(pdf)[0]
            if pdf_name + ".txt" in txts:
                pdf_pairs.append((pdf, pdf_name + ".txt"))
            else:
                raise FileNotFoundError(f"Text file for {pdf} not found")
        cls.pdfs = pdf_pairs

        cls.tmp_dir = os.path.join(os.path.dirname(__file__), "tmp")
        os.makedirs(cls.tmp_dir, exist_ok=True)

    def test_run(self):
        for source_pdf, expected_txt in self.pdfs:
            with self.subTest(source_pdf=source_pdf, expected_txt=expected_txt):
                extractor = PdfExtractor()
                result_file = extractor.extract(source_pdf, self.tmp_dir)
                self.assertTrue(os.path.exists(result_file))
                with open(result_file, 'r', encoding='utf-8') as result, open(expected_txt, 'r', encoding='utf-8') as expected:
                    result_text = result.read()
                    expected_text = expected.read()
                    diff = list(difflib.unified_diff(result_text.splitlines(), expected_text.splitlines(), lineterm=''))
                    if diff:
                        self.fail('\n'.join(diff))
