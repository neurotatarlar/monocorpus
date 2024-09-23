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
    ENTRY_POINT = "000_entry_point"
    BOOKS_CACHE = "010_books_cache"
    PAGE_IMAGES = '100_page_images'
    LABEL_STUDIO_TASKS = "200_label_studio_tasks"
    BOXES_PLOTS = '300_boxes_plots'
    ANNOTATION_RESULTS = '500_annotation_results'

    ARTIFACTS = '900_artifacts'
    ANNOTATIONS = '910_annotations'
    DOCS_PLOT = '920_docs_plots'

