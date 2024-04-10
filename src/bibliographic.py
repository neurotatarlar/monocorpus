import os

import typer
from post_processor import normalize_word
from nltk import edit_distance
from itertools import groupby
from collections import OrderedDict
from consts import Dirs
from file_utils import get_real_path_to_dir_with_executed_script


# todo autocompletion for author
# todo confirm in any layout
# todo replace cyrillic chars in latin words

WRITERS_LIST_FILE = 'writers_list.txt'

def prompt_bibliographic_info():
    author = ''
    title = ''
    while True:
        author = typer.prompt("Enter book's author. If there is no author, just press Enter",
                              default=author if author else '')
        title = typer.prompt("Enter book's title. If there is no title, just press Enter  ",
                             default=title if title else '')
        if not author and not title:
            typer.secho("Author and title are empty. Please provide at least one of them", fg=typer.colors.RED)
            continue

        author = _normalize(author, capitalize=True)
        title = _normalize(title, capitalize=False)

        closest_author = _find_closest(author)
        if closest_author and closest_author != author:
            if typer.confirm(f"Closest author found is '{closest_author}'. Replace with this author?", default=True):
                author = closest_author
            else:
                append_writer([author])
                
        author = '_'.join(author.split())
        title = '_'.join(title.split())

        normalized_name = author + "__" + title if author and title else author or title

        if typer.confirm(f"Normalized name is '{normalized_name}'. Continue with this name?", default=True):
            return normalized_name


def get_writers_list():
    path = os.path.join(Dirs.WORKDIR.get_real_path(), WRITERS_LIST_FILE)
    with open(path) as f:
        writers = f.read().splitlines()
    return writers


def append_writer(writers):
    path = os.path.join(Dirs.WORKDIR.get_real_path(), WRITERS_LIST_FILE)
    with open(path, 'a') as f:
        for w in writers:
            f.write(f'{w}')


def _find_closest(author, distance_threshold=3):
    writers = get_writers_list()
    min_distance = 128
    closest = None
    for w in writers:
        distance = edit_distance(author, w)
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