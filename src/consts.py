from enum import Enum

"""Constants for the project"""

_TATAR_SPECIFIC_CHARS_CAPITAL = {'Ә', 'Ү', 'Ө', 'Җ', 'Ң', 'Һ'}

_TATAR_SPECIFIC_CHARS_LOWERCASE = {'ә', 'ү', 'ө', 'җ', 'ң', 'һ'}

"""Characters can be found only in Tatar language"""
TATAR_SPECIFIC_CHARS = _TATAR_SPECIFIC_CHARS_LOWERCASE.union(_TATAR_SPECIFIC_CHARS_CAPITAL)

_RUSSIAN_ALPHABET_CAPITAL = {'А', 'Б', 'В', 'Г', 'Д', 'Е', 'Ё', 'Ж', 'З', 'И', 'Й', 'К', 'Л', 'М', 'Н', 'О', 'П', 'Р',
                             'С', 'Т', 'У', 'Ф', 'Х', 'Ц', 'Ч', 'Ш', 'Щ', 'Ъ', 'Ы', 'Ь', 'Э', 'Ю', 'Я'}

_RUSSIAN_ALPHABET_LOWERCASE = {'а', 'б', 'в', 'г', 'д', 'е', 'ё', 'ж', 'з', 'и', 'й', 'к', 'л', 'м', 'н', 'о', 'п', 'р',
                               'с', 'т', 'у', 'ф', 'х', 'ц', 'ч', 'ш', 'щ', 'ъ', 'ы', 'ь', 'э', 'ю', 'я'}

"""Full Tatar alphabet with capital and lowercase letters"""
TATAR_CYRILLIC_ALPHABET = TATAR_SPECIFIC_CHARS.union(_RUSSIAN_ALPHABET_LOWERCASE).union(_RUSSIAN_ALPHABET_CAPITAL)

"""Non-alphanumeric characters that legit to be in the text"""
VALID_NON_ALPHA_NUMERIC = {' ', '/', '\\', '"', "'", '@', '#', '$', '%', '^', '&', '*', '(', ')', '*', '-', '—',
                           '_', '+', '=', '{', '}', '[', ']', ';', ':', '<', ',', '>', '.', '?', '!', '№',
                           '|', '\t', '\n', '~', '©', '™', '§', '°', '€', '£', '¥', '₽', '₴', '₸', '₺', '₿'}

DIGITS = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9'}

TATAR_ALPHA_NUMERIC = TATAR_CYRILLIC_ALPHABET.union(DIGITS)


class Dirs(Enum):
    """
    Enum with all the directories and files that are used in the project

    - ENTRY_POINT: Directory where all the files begins their journey
    - WORK_IN_PROGRESS: Directory where files are moved then they are processed
    - PAGE_IMAGES: Directory where images of the pages are stored
    - LABEL_STUDIO_TASKS: Directory where Label Studio tasks are stored
    - NOT_A_DOCUMENT: Directory where files that are not documents at all are moved (e.g. images, archives)
    - NOT_SUPPORTED_FORMAT_YET: Directory where files with formats that are not supported yet are moved
    - NOT_TATAR: Directory where files that are not in Tatar language are moved
    - ARTIFACTS: Directory where all processing artifacts(e.g. txt files with extracted text) are stored
    - COMPLETED: Directory where files that are processed successfully are moved
    """
    ENTRY_POINT = "000_entry_point"
    WORK_IN_PROGRESS = "001_work_in_progress"
    PAGE_IMAGES = '100_page_images'
    LABEL_STUDIO_TASKS = "200_label_studio_tasks"

    NOT_A_DOCUMENT = "500_not_a_document"
    NOT_SUPPORTED_FORMAT_YET = "510_not_supported_yet"
    NOT_TATAR = "520_not_tt_document"

    ARTIFACTS = '900_artifacts'
    EXTRACTED_DOCS = '910_extracted_docs'
