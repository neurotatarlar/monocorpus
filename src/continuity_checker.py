import re

from spacy.lang.tt import Tatar

nlp = Tatar()
nlp.add_pipe('sentencizer')

HEADER_PATTERNS = [
    r'^\s*<\s*/?\s*\w+',                    # HTML tag
    r'^[*#=-]+\s*\d*\.*\s*[A-ZА-ЯӘӨҮҢҖҺЁ]', # Markdown + uppercase start
    r'^\d+[\.\)]\s+[A-ZА-ЯӘӨҮҢҖҺЁ]',        # "1. Title", "2) Title"
    r'^[IVXLCDM]+\.\s+[A-ZА-ЯӘӨҮҢҖҺЁ]',     # Roman numeral sections like "IV. Results"
]

def continue_smoothly(prev_chunk_tail, content):
    """
    Check if two paragraphs are the same
    return: True if paragraphs are the same, False otherwise
    """
    content_head = content[:300]
    for pattern in HEADER_PATTERNS:
        if re.match(pattern, content_head):
            return '\n\n' + content

    nlp_prev = nlp(prev_chunk_tail)
    nlp_new = nlp(content_head)
    last_sent = list(nlp_prev.sents)[-1].text
    first_sent = list(nlp_new.sents)[0].text
    nlp_merged = nlp(last_sent + first_sent)
    same_paragraph = len(list(nlp_merged.sents)) == 1
    if same_paragraph:
        if prev_chunk_tail.endswith('-'):
            return content
        else:
            return ' ' + content
    else:
        return '\n\n' + content
        