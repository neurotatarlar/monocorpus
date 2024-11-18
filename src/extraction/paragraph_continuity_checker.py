from spacy.lang.tt import Tatar
import re

nlp = Tatar()
nlp.add_pipe('sentencizer')


def check_paragraphs_are_the_same(prev, new):
    """
    Check if two paragraphs are the same
    return: True if paragraphs are the same, False otherwise
    """
    # Another workaround. Allows not accidentally merge paragraph with a header
    if re.match('^#+ ', prev):
        return False

    nlp_prev = nlp(prev)
    nlp_new = nlp(new)
    nlp_merged = nlp(f"{list(nlp_prev.sents)[-1].__repr__()}{list(nlp_new.sents)[0].__repr__()}")

    if len(list(nlp_merged.sents)) > 1:
        return False
    else:
        return True
