"""
This module is responsible for counting words in the monocorpus dataset.
"""
import re
from functools import reduce

import pandas as pd
from huggingface_hub import snapshot_download
from rich import print
from rich.progress import track

from file_utils import pick_files

CRAWLED_DOCS_REPO_ID = "neurotatarlar/tt-crawl"
BOOKS_TEXTS_REPO_ID = "neurotatarlar/tt-books-cyrillic"
COLUMNS = ["title", "article_text"]
WORD_REGEX = "[A-Za-zА-Яа-яӨөәҢүһҗҖҮңҺӘ]+"


def words_counter():
    """
    Count words in the both crawled documents and extracted from books texts
    """
    words_in_book_texts = _count_words_in_texts_from_books()
    words_in_crawled_docs = _count_words_in_crawled_docs()
    overall = words_in_book_texts + words_in_crawled_docs
    return overall, words_in_book_texts, words_in_crawled_docs


def _count_words_in_texts_from_books():
    print(f"Downloading the dataset '{BOOKS_TEXTS_REPO_ID}' from the Hugging Face...")
    snapshot = snapshot_download(repo_id=BOOKS_TEXTS_REPO_ID, allow_patterns=["*.txt"], repo_type="dataset")
    all_txt_files = pick_files(snapshot, -1)
    total_words = 0
    pattern = re.compile(WORD_REGEX)
    for file in track(all_txt_files, description="Counting words in the texts extracted from the books"):
        with open(file, "r") as f:
            total_words += reduce(lambda x, y: x + y, [len(pattern.findall(line)) for line in f.readlines()])
    return total_words


def _count_words_in_crawled_docs():
    print(f"Downloading the dataset '{CRAWLED_DOCS_REPO_ID}' from the Hugging Face...")
    snapshot = snapshot_download(repo_id=CRAWLED_DOCS_REPO_ID, allow_patterns=["*.parquet"], repo_type="dataset")
    print(f"Reading the dataset '{CRAWLED_DOCS_REPO_ID}'...")
    df = pd.read_parquet(snapshot, columns=COLUMNS)
    print("Calculating the number of words in the crawled documents...")
    return int(reduce(lambda x, y: x + y, [df[column].str.count(WORD_REGEX).sum() for column in COLUMNS]))
