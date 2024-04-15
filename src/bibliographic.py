import os

from nltk import edit_distance

from cli_wrapper import prompt, secho, confirm_prompt
from consts import Dirs
from post_processor import normalize_word

WRITERS_LIST_FILE_NAME = 'writers_list.txt'


def prompt_bibliographic_info() -> tuple[str, str, str]:
    """
    Prompt user for the bibliographic information about the book
    :return: book's author, book's title, normalized name
    """
    author = ''
    title = ''
    while True:
        author = prompt("Enter book's author. If there is no author, just press Enter",
                        default=author if author else '')
        title = prompt("Enter book's title. If there is no title, just press Enter  ",
                       default=title if title else '')
        if not author and not title:
            secho(message="Author and title are empty. Please provide at least one of them")
            continue

        author = _normalize(author, capitalize=True)
        title = _normalize(title, capitalize=False)

        closest_author = _find_closest(author)
        if closest_author and closest_author != author:
            if confirm_prompt(f"Closest author found is '{closest_author}'. Replace with this author?"):
                author = closest_author
            else:
                append_writer([author])

        author = '_'.join(author.split())
        title = '_'.join(title.split())

        normalized_name = author + "__" + title if author and title else author or title

        if confirm_prompt(f"Normalized name is '{normalized_name}'. Continue with this name?"):
            return author, title, normalized_name


def get_writers_list():
    """
    Get the list of writers from the file
    :return: list of writers
    """
    path = os.path.join(Dirs.WORKDIR.get_real_path(), WRITERS_LIST_FILE_NAME)
    with open(path) as f:
        writers = f.read().splitlines()
    return writers


def append_writer(writers):
    """
    Append new writers to the list of writers
    :param writers: list of writers
    """
    path = os.path.join(Dirs.WORKDIR.get_real_path(), WRITERS_LIST_FILE_NAME)
    with open(path, 'a') as f:
        for w in writers:
            f.write(f'{w}')


def _find_closest(author, distance_threshold=3):
    writers = get_writers_list()
    min_distance = 128
    closest = None
    l_author = author.lower()
    for w in writers:
        distance = edit_distance(l_author, w.lower())
        if distance < min_distance:
            min_distance = distance
            closest = w

    if min_distance <= distance_threshold:
        return closest
    else:
        return None


def _normalize(words, capitalize):
    words = words.split()
    tmp = []
    for w in words:
        _, _, nw = normalize_word(w)
        tmp.append(nw.capitalize() if capitalize else nw)
    return " ".join(tmp)
