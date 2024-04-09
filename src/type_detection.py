from enum import Enum

import magic


class FileType(Enum):
    PDF = 1,
    EPUB = 2,
    FB2 = 3,
    DJVU = 4,
    OTHER = 9,


def detect_type(path_to_file: str):
    """
    Detects type of the file by its mime type

    :param path_to_file: path there file is located
    :return: one of the FileType enum values
    """
    mime_type = magic.from_file(path_to_file, mime=True)
    match mime_type:
        case 'application/pdf' | 'application/x-pdf':
            return FileType.PDF
        case 'application/epub+zip':
            return FileType.EPUB
        case 'text/xml':
            return FileType.FB2
        case _:
            return FileType.OTHER
