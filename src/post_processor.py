import os.path

from consts import TATAR_SPECIFIC_CHARS, Dirs, TATAR_ALPHA_NUMERIC, VALID_NON_ALPHA_NUMERIC, \
    EXPECTED_CHARS

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


def post_process(path_to_txt_file):
    """
    Post-processes the text file

    What do we consider as post-processing:
    - Replace look-alike chars with the chars of Tatar alphabet, eg 'e' (english alphabet) -> 'е'(Tatar cyrillic alphabet)
    - Replace non-alphanumeric chars with the valid ones, eg '‘' -> "'"
    - Check if the document is in Tatar language

    :param path_to_txt_file:
    :return:
    """
    print(f"Post-processing file: {path_to_txt_file}")

    total_chars_count = 0
    total_valid_chars_count = 0
    total_tatar_specific_chars_count = 0
    new_path = os.path.join(Dirs.ARTIFACTS.get_real_path(), os.path.basename(path_to_txt_file))
    with open(path_to_txt_file, 'r', encoding="utf-8") as input, open(new_path, 'w', encoding="utf-8") as output:
        word = []
        while ch := input.read(1):
            match ch:
                case _ if ch.isalnum() is False:  # we met end of the word or non-alphanumeric char
                    # count the word separator
                    total_valid_chars_count += 1
                    if word:
                        # made up the word
                        word_str = ''.join(word)
                        # normalize the word by replacing look-alike chars with the chars of Tatar alphabet
                        valid_chars_count, tatar_specific_chars_count, normalized_word = _normalize_word(word_str)

                        total_valid_chars_count += valid_chars_count
                        total_tatar_specific_chars_count += tatar_specific_chars_count

                        output.write(normalized_word)
                        word.clear()

                    if replaced_char := _replace_nonalphanum_chars(ch):
                        output.write(replaced_char)

                case _:  # we are in the middle of the word, just append the char to the word until word separator met
                    word.append(ch)
            total_chars_count += 1

    return _check_doc_in_tatar_language(total_chars_count, total_valid_chars_count, total_tatar_specific_chars_count)


def _check_doc_in_tatar_language(total_chars_count, total_valid_chars_count, total_tatar_specific_chars_count):
    """
    Check if the document is in Tatar language

    We consider the document as Tatar if:
    - the coefficient of valid Tatar chars in the document is greater than minimal_valid_chars_threshold. In other words
    the document must consist of at least 95% of valid Tatar chars
    - the coefficient of Tatar specific chars in the document is greater than minimal_Tatar_specific_chars_threshold.
    In other words, the document must consist of at least 3% of Tatar specific chars. We need to check this as well
    because Tatar Cyrillic alphabet is enhancement of Russian alphabet, so with the first check any Russian document
    will be considered as Tatar. Additional check to Tatar unique chars is needed to avoid this.


    :param total_chars_count:
    :param total_valid_chars_count:
    :param total_Tatar_specific_chars_count:
    :return:
    """
    return (total_valid_chars_count / total_chars_count >= MINIMAL_VALID_THRESHOLD and
            total_tatar_specific_chars_count / total_chars_count >= MINIMAL_TATAR_SPECIFIC_CHARS_THRESHOLD)


def _normalize_word(word):
    """
    Normalize the word by replacing look-alike chars with the chars of Tatar alphabet

    :param word: word to normalize
    :return:
    """

    # count of chars than we expect to be in Tatar word (see consts.EXPECTED_CHARS)
    valid_tatar_chars_in_word = 0
    # count of chars that are specific for Tatar language and were found in the word (see consts.TATAR_SPECIFIC_CHARS)
    tatar_specific_chars_in_word = 0

    for ch in word:
        if ch in EXPECTED_CHARS:
            valid_tatar_chars_in_word += 1
        if ch in TATAR_SPECIFIC_CHARS:
            tatar_specific_chars_in_word += 1

    # coefficient of valid Tatar chars in the word
    valid_tatar_chars_in_word_coef = valid_tatar_chars_in_word / len(word)

    if valid_tatar_chars_in_word_coef == 1.0:
        # the word fully consists of valid Tatar chars, so no transformation needed
        result = word
    elif valid_tatar_chars_in_word_coef == 0.0:
        # the word fully consists of non-Tatar chars, so this is not a tatar word, just return it as is
        result = word
    elif valid_tatar_chars_in_word_coef >= MINIMAL_VALID_CHARS_IN_WORD_THRESHOLD:
        # the word consists of both Tatar and non-tatar chars, but Tatar chars are major, so we need to transform it
        print(f"Found anomaly in word '{word}', tatarish coef={valid_tatar_chars_in_word_coef}")
        buf = []
        for pos, original_ch in enumerate(word):
            replaced_ch = _replace_tatar_char_look_alikes(original_ch)
            buf.append(replaced_ch)
            if original_ch != replaced_ch:
                print(
                    f"In word '{word}' replaced char `{original_ch}`({hex(ord(original_ch))}) "
                    f"with `{replaced_ch}`({hex(ord(replaced_ch))})"
                )
        result = "".join(buf)

    else:
        # the word consists of both Tatar and non-Tatar chars, but non-Tatar
        # chars are major, so we consider it as non-Tatar word, just return it as is
        # todo should we replace Tatar chars with non-Tatar ones?
        print(f"Found anomaly in word '{word}', tatarish coef is {valid_tatar_chars_in_word_coef}")
        result = word
    return valid_tatar_chars_in_word, tatar_specific_chars_in_word, result


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
        case 'k' | 'ķ' | 'ĸ' | 'қ' | 'ҡ' | 'ҟ' | 'ҝ' | 'ҡ' | 'ҟ' | 'ҝ' | 'қ' | 'ҡ' | 'ҟ' | 'ҝ':
            return 'к'
        case 'K' | 'Ķ' | 'Қ' | 'Ҡ' | 'Ҟ' | 'Ҝ' | 'Ҡ' | 'Ҟ' | 'Ҝ' | 'Қ' | 'Ҡ' | 'Ҟ' | 'Ҝ':
            return 'К'
        case 'ґ' | 'ғ':
            return 'г'
        case 'Ґ' | 'Ғ':
            return 'Г'
        case _ if char not in TATAR_ALPHA_NUMERIC:
            print(f"Unexpected char: `{char}`({hex(ord(char))}), please cover this char as well")
            return char
        case _:
            return char


def _replace_nonalphanum_chars(char):
    """
    Replacing non-valid non-alphanumeric chars to the valid ones.
    The valid ones are defined in the VALID_NON_ALPHA_NUMERIC.

    :param char: char to replace
    :return: None if the char must not be in the final text or the replaced char or the original char
    """
    match char:
        case '`' | '‘' | '’':
            return "'"
        case '»' | '«' | '“' | '”' | '„':
            return '"'
        case '×':
            return '*'
        case '–' | '­':  # but not `—`
            return '-'
        case '…':
            return '...'
        case ' ' | '' | ' ' | '':
            return None
        case _ if char not in VALID_NON_ALPHA_NUMERIC:
            print(f"Unexpected char: `{char}`({hex(ord(char))}), it will be removed; if it is relevant char please add "
                  f"it to the list")
            return None
        case _:
            return char
