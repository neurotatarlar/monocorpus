import os.path
import re
import string
from string import whitespace

import typer

from consts import VALID_CHARS

"""
Minimal threshold of valid Tatar chars(see consts.EXPECTED_CHARS) in the document to consider it as Tatar document.
Should be considered together with MINIMAL_TATAR_SPECIFIC_CHARS_THRESHOLD to avoid considering Russian texts as Tatar.
Less than this threshold the document will be considered as non-Tatar document
Greater or equal than this threshold the document will be considered as Tatar document.
"""
MINIMAL_VALID_THRESHOLD = 0.95

"""
Minimal threshold of Tatar specific chars(see consts.TATAR_SPECIFIC_CHARS) in the document to consider it as Tatar 
document. Should be considered together with MINIMAL_VALID_THRESHOLD to avoid considering Russian texts as Tatar.
Less than this threshold the document will be considered as non-Tatar document
Greater or equal than this threshold the document will be considered as Tatar document.
"""

MINIMAL_TATAR_SPECIFIC_CHARS_THRESHOLD = 0.03
"""
Minimal threshold of valid Tatar chars(see consts.EXPECTED_CHARS) in the word to consider it as Tatar word. 
Less than this threshold the word will be considered as non-Tatar word
Greater or equal than this threshold the word will be considered as Tatar word
"""
MINIMAL_VALID_CHARS_IN_WORD_THRESHOLD = 0.5


def post_process(text_block):
    # remove word breaks
    text_block = re.sub(r'­', r'', text_block)
    # remove extra spaces
    text_block = re.sub(r'\s+', r' ', text_block)
    # remove poetry line end
    text_block = re.sub(r'(<#PLE#>)+', '  \n', text_block)
    # remove single whitespace before the punctuations
    text_block = re.sub(r'\s([?.!")\]}])', r'\1', text_block)
    # remove single whitespace after the open brackets
    text_block = re.sub(r'([(\[{§])\s', r'\1', text_block)
    # workaround for enormously big characters at the beginning of the paragraph what detected as a header
    text_block = re.sub(r'^#+\s(.)\s(.*)', r'\1\2', text_block)

    # replace look-alike chars with the chars of Tatar alphabet
    text_block = _replace_look_alikes(text_block)
    text_block = _replace_nonalphanum_chars(text_block)

    # escape unordered markdown list markers
    text_block = re.sub(r'^(\s)?([-*+-])([\s\n])', r'\1\\\2\3', text_block)

    text_block = re.sub(r'^•', r'-', text_block)

    return text_block.strip(whitespace)


def pre_process(text):
    """
    Escape special characters of Markdown
    """
    res = re.sub(r'([#*>`\[\]_{}])', r'\1', text)

    if res != text:
        print(f"Escaped markdown: {text}, {res}")

    return res



def _replace_look_alikes(text):
    """
    Normalize the word by replacing look-alike chars with the chars of Tatar alphabet

    :param text: word to normalize
    :return: tuple of valid Tatar chars count, Tatar specific chars count, normalized word
    """

    result = []
    for w in re.split('(\\W)', text):
        # count of chars that valid (see consts.VALID_CHARS)
        valid_tatar_chars_in_word = 0

        w = _preprocess(w)

        if not w.isascii():
            for ch in w:
                if ch in VALID_CHARS:
                    valid_tatar_chars_in_word += 1

            # coefficient of valid Tatar chars in the word
            valid_tatar_chars_in_word_coef = valid_tatar_chars_in_word / len(w)

            if valid_tatar_chars_in_word_coef == 1.0:
                # the word fully consists of valid Tatar chars, so no transformation needed
                w = w
            elif valid_tatar_chars_in_word_coef == 0.0:
                # the word fully consists of non-Tatar chars, so this is not a tatar word, just return it as is
                w = w
            elif valid_tatar_chars_in_word_coef >= MINIMAL_VALID_CHARS_IN_WORD_THRESHOLD:
                # the word consists of both Tatar and non-tatar chars, but Tatar chars are major, so we need to transform it
                w = _tatarify(w)
            else:
                # the word consists of both Tatar and non-Tatar chars, but non-Tatar chars are major
                w = _de_tatarify(w)

        result.append(w)

    return "".join(result)


def _tatarify(word):
    buf = []
    for original_ch in word:
        replaced_ch = _replace_tatar_char_look_alikes(original_ch)
        if original_ch != replaced_ch:
            typer.echo(
                f"In word '{word}' replaced not tatar char '{original_ch}'({original_ch.encode('unicode_escape')}) "
                f"with tatar '{replaced_ch}'({replaced_ch.encode('unicode_escape') if replaced_ch else None})"
            )
        buf.append(replaced_ch)
    return "".join(buf)


def _de_tatarify(word):
    """
    Replace tatar chars in non-tatar word with the ASCII chars
    This can happen during OCR, when the text is recognized as Tatar, but it's not

    :param word: word to de-tatarify
    """
    buf = []
    for original_ch in word:
        replaced_ch = _replace_ascii_look_alikes(original_ch)
        if original_ch != replaced_ch:
            typer.echo(
                f"In word '{word}' replaced non-ASCII char '{original_ch}'({hex(ord(original_ch))}) "
                f"with '{replaced_ch}'({hex(ord(replaced_ch)) if replaced_ch else None})"
            )
        buf.append(replaced_ch)
    return "".join(buf)


def _replace_tatar_char_look_alikes(char):
    """
    Replacing look-alike chars with the chars of Tatar alphabet

    :param char: char to replace
    :return: replaced char or the original char
    """
    match char:
        # vowels
        case 'ə':
            return 'ә'
        case 'Ə':
            return 'Ә'
        case 'e' | 'ė' | 'ĕ' | 'ẹ' | 'ė' | 'é' | 'è' | 'ê' | 'ë' | 'ē' | 'ě' | 'ę' | 'ẽ' | 'ẻ' | 'ȅ' | 'ȇ' | 'ẹ' | 'ẻ' | 'ẽ' | 'ế' | 'ề' | 'ể' | 'ễ' | 'ệ':
            return 'е'
        case 'E' | 'Ė' | 'Ĕ' | 'Ẹ' | 'Ė' | 'É' | 'È' | 'Ê' | 'Ë' | 'Ē' | 'Ě' | 'Ę' | 'Ẽ' | 'Ẻ' | 'Ȅ' | 'Ȇ' | 'Ẹ' | 'Ẻ' | 'Ẽ' | 'Ế' | 'Ề' | 'Ể' | 'Ễ' | 'Ệ':
            return 'Е'
        case 'o' | 'ọ' | 'ő' | 'ô' | 'ö' | 'ò' | 'ó' | 'õ' | 'ø' | 'ō' | 'ŏ' | 'ȯ' | 'ȱ' | 'ọ' | 'ỏ' | 'õ' | 'ô' | 'ố' | 'ồ' | 'ổ' | 'ỗ' | 'ộ':
            return 'о'
        case 'O' | 'Ọ' | 'Ő' | 'Ô' | 'Ö' | 'Ò' | 'Ó' | 'Õ' | 'Ø' | 'Ō' | 'Ŏ' | 'Ȯ' | 'Ȱ' | 'Ọ' | 'Ỏ' | 'Õ' | 'Ô' | 'Ố' | 'Ồ' | 'Ổ' | 'Ỗ' | 'Ộ':
            return 'О'
        case 'a' | 'à' | 'á' | 'â' | 'ã' | 'ä' | 'å' | 'ā' | 'ă' | 'ą' | 'ȁ' | 'ȃ' | 'ȧ' | 'ȩ' | 'ǎ' | 'ǟ' | 'ǡ' | 'ǻ' | 'ȁ' | 'ȃ' | 'ȧ' | 'ȩ' | 'ǎ' | 'ǟ' | 'ǡ' | 'ǻ':
            return 'а'
        case 'A' | 'À' | 'Á' | 'Â' | 'Ã' | 'Ä' | 'Å' | 'Ā' | 'Ă' | 'Ą' | 'Ȁ' | 'Ȃ' | 'Ȧ' | 'Ȩ' | 'Ǎ' | 'Ǟ' | 'Ǡ' | 'Ǻ' | 'Ȁ' | 'Ȃ' | 'Ȧ' | 'Ȩ' | 'Ǎ' | 'Ǟ' | 'Ǡ' | 'Ǻ':
            return 'А'
        case 'y' | 'ý' | 'ỳ' | 'ỹ' | 'ỷ' | 'ỵ' | 'ȳ' | 'ÿ' | 'ỳ' | 'ý' | 'ỹ' | 'ỷ' | 'ỵ' | 'ȳ' | 'ÿ':
            return 'у'
        case 'Y' | 'Ý' | 'Ỳ' | 'Ỹ' | 'Ỷ' | 'Ỵ' | 'Ȳ' | 'Ÿ' | 'Ỳ' | 'Ý' | 'Ỹ' | 'Ỷ' | 'Ỵ' | 'Ȳ' | 'Ÿ':
            return 'У'

        # consonants
        case 'c' | 'ƈ' | 'ċ':
            return 'с'
        case 'C' | 'Ƈ' | 'Ċ':
            return 'С'
        case 'x' | 'ẋ' | 'ẍ' | 'ҳ' | 'ӽ' | 'ӿ' | 'Ӽ' | 'Ӿ' | 'ӽ':
            return 'х'
        case 'X' | 'Ẋ' | 'Ẍ' | 'Ҳ' | 'Ӽ' | 'Ӽ' | 'Ӿ' | 'Ӽ' | 'Ӽ':
            return 'Х'
        case 'p' | 'ṗ' | 'ṕ':
            return 'р'
        case 'P' | 'Ṗ' | 'Ṕ':
            return 'Р'
        case 'h' | 'ħ' | 'ḥ' | 'ḧ' | 'ḩ' | 'ḫ' | 'ḣ' | 'ḥ' | 'ḧ' | 'ḩ' | 'ḫ' | 'ḣ':
            return 'һ'
        case 'H' | 'Ħ' | 'Ḥ' | 'Ḧ' | 'Ḩ' | 'Ḫ' | 'Ḣ' | 'Ḥ' | 'Ḧ' | 'Ḩ' | 'Ḫ' | 'Ḣ':
            return 'Һ'
        case 'k' | 'ķ' | 'ĸ' | 'қ' | 'ҡ' | 'ҟ' | 'ҝ' | 'ҡ' | 'ҟ' | 'ҝ' | 'қ' | 'ҡ' | 'ҟ' | 'ҝ' | 'ќ':
            return 'к'
        case 'K' | 'Ķ' | 'Қ' | 'Ҡ' | 'Ҟ' | 'Ҝ' | 'Ҡ' | 'Ҟ' | 'Ҝ' | 'Қ' | 'Ҡ' | 'Ҟ' | 'Ҝ' | 'Ќ':
            return 'К'
        case 'ґ' | 'ғ':
            return 'г'
        case 'Ґ' | 'Ғ':
            return 'Г'
        case _:
            return char


def _replace_ascii_look_alikes(char):
    """
    Replacing non-ASCII look-alike chars with the ASCII chars

    :param char: char to replace
    :return: replaced char or the original char
    """
    match char:
        case 'а' | 'à' | 'á' | 'â' | 'ã' | 'ä' | 'å' | 'ā' | 'ă' | 'ą' | 'ȁ' | 'ȃ' | 'ȧ' | 'ȩ' | 'ǎ' | 'ǟ' | 'ǡ' | 'ǻ' | 'ȁ' | 'ȃ' | 'ȧ' | 'ȩ' | 'ǎ' | 'ǟ' | 'ǡ' | 'ǻ':
            return 'a'
        case 'А' | 'À' | 'Á' | 'Â' | 'Ã' | 'Ä' | 'Å' | 'Ā' | 'Ă' | 'Ą' | 'Ȁ' | 'Ȃ' | 'Ȧ' | 'Ȩ' | 'Ǎ' | 'Ǟ' | 'Ǡ' | 'Ǻ' | 'Ȁ' | 'Ȃ' | 'Ȧ' | 'Ȩ' | 'Ǎ' | 'Ǟ' | 'Ǡ' | 'Ǻ':
            return 'A'
        case 'с' | 'ƈ' | 'ċ':
            return 'c'
        case 'С' | 'Ƈ' | 'Ċ':
            return 'C'
        case 'ԁ ' | 'ɗ':
            return 'd'
        case 'е' | 'ė' | 'ĕ' | 'ẹ' | 'ė' | 'é' | 'è' | 'ê' | 'ë' | 'ē' | 'ě' | 'ę' | 'ẽ' | 'ẻ' | 'ȅ' | 'ȇ' | 'ẹ' | 'ẻ' | 'ẽ' | 'ế' | 'ề' | 'ể' | 'ễ' | 'ệ':
            return 'e'
        case 'Е' | 'Ė' | 'Ĕ' | 'Ẹ' | 'Ė' | 'É' | 'È' | 'Ê' | 'Ë' | 'Ē' | 'Ě' | 'Ę' | 'Ẽ' | 'Ẻ' | 'Ȅ' | 'Ȇ' | 'Ẹ' | 'Ẻ' | 'Ẽ' | 'Ế' | 'Ề' | 'Ể' | 'Ễ' | 'Ệ':
            return 'E'
        case 'ġ':
            return 'g'
        case 'Ġ':
            return 'G'
        case 'һ' | 'ħ' | 'ḥ' | 'ḧ' | 'ḩ' | 'ḫ' | 'ḣ' | 'ḥ' | 'ḧ' | 'ḩ' | 'ḫ' | 'ḣ':
            return 'h'
        case 'Һ' | 'Ħ' | 'Ḥ' | 'Ḧ' | 'Ḩ' | 'Ḫ' | 'Ḣ' | 'Ḥ' | 'Ḧ' | 'Ḩ' | 'Ḫ' | 'Ḣ':
            return 'H'
        case 'і' | 'í' | 'ï':
            return 'i'
        case 'І' | 'Í' | 'Ï':
            return 'I'
        case 'ј' | 'ʝ':
            return 'j'
        case 'Ј' | 'ʝ':
            return 'J'
        case 'к' | 'ķ' | 'ĸ' | 'қ' | 'ҡ' | 'ҟ' | 'ҝ' | 'ҡ' | 'ҟ' | 'ҝ' | 'қ' | 'ҡ' | 'ҟ' | 'ҝ' | 'ќ':
            return 'k'
        case 'К' | 'Ķ' | 'Қ' | 'Ҡ' | 'Ҟ' | 'Ҝ' | 'Ҡ' | 'Ҟ' | 'Ҝ' | 'Қ' | 'Ҡ' | 'Ҟ' | 'Ҝ' | 'Ќ':
            return 'K'
        case 'ӏ' | 'ḷ':
            return 'l'
        case 'Ӏ' | 'Ḷ':
            return 'L'
        case 'ո':
            return 'n'
        case 'о' | 'ọ' | 'ő' | 'ô' | 'ö' | 'ò' | 'ó' | 'õ' | 'ø' | 'ō' | 'ŏ' | 'ȯ' | 'ȱ' | 'ọ' | 'ỏ' | 'õ' | 'ô' | 'ố' | 'ồ' | 'ổ' | 'ỗ' | 'ộ':
            return 'o'
        case 'О' | 'Ọ' | 'Ő' | 'Ô' | 'Ö' | 'Ò' | 'Ó' | 'Õ' | 'Ø' | 'Ō' | 'Ŏ' | 'Ȯ' | 'Ȱ' | 'Ọ' | 'Ỏ' | 'Õ' | 'Ô' | 'Ố' | 'Ồ' | 'Ổ' | 'Ỗ' | 'Ộ':
            return 'O'
        case 'р' | 'ṗ' | 'ṕ':
            return 'p'
        case 'Р' | 'Ṗ' | 'Ṕ':
            return 'P'
        case 'զ' | 'ɋ':
            return 'q'
        case 'Ԛ' | 'ԛ':
            return 'Q'
        case 'ʂ' | 'ṡ' | 'ṣ' | 'ṩ' | 'ṥ' | 'ṧ' | 'ṣ' | 'ṩ' | 'ṥ' | 'ṧ' | 'ṣ':
            return 's'
        case 'Ѕ' | 'Ѕ':
            return 'S'
        case 'υ' | 'ս' | 'ü' | 'ú' | 'ù':
            return 'u'
        case 'Ս' | 'Ü' | 'Ú' | 'Ù':
            return 'U'
        case 'ν' | 'ѵ':
            return 'v'
        case 'Ѵ' | 'ѵ':
            return 'V'
        case 'х' | 'ẋ' | 'ẍ' | 'ҳ' | 'ӽ' | 'ӿ' | 'Ӽ' | 'Ӿ' | 'ӽ':
            return 'x'
        case 'Х' | 'Ẋ' | 'Ẍ' | 'Ҳ' | 'Ӽ' | 'Ӽ' | 'Ӿ' | 'Ӽ' | 'Ӽ':
            return 'X'
        case 'у' | 'ý' | 'ỳ' | 'ỹ' | 'ỷ' | 'ỵ' | 'ȳ' | 'ÿ' | 'ỳ' | 'ý' | 'ỹ' | 'ỷ' | 'ỵ' | 'ȳ' | 'ÿ':
            return 'y'
        case 'У' | 'Ý' | 'Ỳ' | 'Ỹ' | 'Ỷ' | 'Ỵ' | 'Ȳ' | 'Ÿ' | 'Ỳ' | 'Ý' | 'Ỹ' | 'Ỷ' | 'Ỵ' | 'Ȳ' | 'Ÿ':
            return 'Y'
        case 'ẕ' | 'ẑ' | 'ʐ' | 'ʑ' | 'ż' | 'ź' | 'ẓ' | 'ẕ' | 'ẑ' | 'ʐ' | 'ʑ' | 'ż' | 'ź' | 'ẓ':
            return 'z'
        case 'Ẑ' | 'ẑ' | 'ʐ' | 'ʑ' | 'Ż' | 'Ź' | 'Ẓ' | 'Ẕ' | 'Ẑ' | 'ẑ' | 'ʐ' | 'ʑ' | 'Ż' | 'Ź' | 'Ẓ':
            return 'Z'
        case _:
            return char


def _replace_nonalphanum_chars(word):
    """
    Replacing non-valid non-alphanumeric chars to the valid ones.
    The valid ones are defined in the VALID_NON_ALPHA_NUMERIC.

    :param char: char to replace
    :return: None if the char must not be in the final text or the replaced char or the original char
    """

    def replace(ch):
        match ch:
            case 'ˆ':
                return '^'
            case '‘' | '’' | '′' | '\u0301':
                return "'"
            case '“' | '”' | '„':
                return '"'
            case '–'  | '‐' | '−':  # but not `—`
                return '-'
            case '…':
                return '...'
            case ' ' | ' ' | '' | '​':
                return None
            case _:
                return ch

    buf = []
    for ch in word:
        if replaced_ch := replace(ch):
            buf.append(replaced_ch)

    return "".join(buf)


def _preprocess(word):
    """
    Preprocess the word before normalization by replacing OCR artifacts

    :param word:
    :return:
    """
    buf = []
    for ch in word:
        match ch:
            case 'њ':
                buf.append('ү')
            case 'Њ':
                buf.append('Ү')
            case 'ћ':
                buf.append('ң')
            case 'Ћ':
                buf.append('Ң')
            case 'ђ':
                buf.append('ә')
            case 'Ђ':
                buf.append('Ә')
            case 'џ':
                buf.append('һ')
            case 'Џ':
                buf.append('Һ')
            case 'љ':
                buf.append('ө')
            case 'Љ':
                buf.append('Ө')
            case _:
                buf.append(ch)
    return "".join(buf)
