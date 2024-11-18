import string
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


VALID_CHARS = TATAR_CYRILLIC_ALPHABET.union(string.digits).union(VALID_NON_ALPHA_NUMERIC)


class Dirs(Enum):
    ENTRY_POINT = "0_entry_point"
    BOOKS_CACHE = "misc/books"
    PAGE_IMAGES = 'misc/page_images'
    LABEL_STUDIO_TASKS = "misc/label-studio-tasks"
    BOXES_PLOTS = 'misc/boxes-plots'
    ANNOTATION_RESULTS = 'misc/annotation-results'

    ARTIFACTS = 'results'
    ANNOTATIONS = 'misc/annotations'
    DOCS_PLOT = 'misc/docs_plots'
