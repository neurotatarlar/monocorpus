"""
This module is responsible for counting words in the monocorpus dataset.
"""
import re
from functools import reduce

import pandas as pd
import tiktoken
from huggingface_hub import snapshot_download
from rich.progress import Progress, track

from file_utils import pick_files

CRAWLED_DOCS_REPO_IDS = ["neurotatarlar/tt-crawl", "veryrealtatarperson/tt-azatliq-crawl"]
BOOKS_TEXTS_REPO_ID = "neurotatarlar/tt-books-cyrillic"
COLUMNS = ["title", "article_text", "article_summary"]
WORD_REGEX = "\\w+"
encoding = tiktoken.get_encoding('o200k_base')


def words_counter(with_tokens):
    """
    Count words in the both crawled documents and extracted from books texts
    """
    words_in_book_texts, tokens_in_book_texts = _count_words_in_texts_from_books(with_tokens)
    words_in_crawled_docs, tokens_in_crawled_texts = _count_words_in_crawled_docs(with_tokens)
    return (words_in_book_texts, words_in_crawled_docs), (tokens_in_book_texts, tokens_in_crawled_texts)


def _count_words_in_texts_from_books(with_tokens):
    snapshot = snapshot_download(repo_id=BOOKS_TEXTS_REPO_ID, allow_patterns=["*.txt"], repo_type="dataset")
    all_txt_files = pick_files(snapshot, -1)
    total_words = 0
    total_tokens = 0
    pattern = re.compile(WORD_REGEX)

    for file in track(all_txt_files, f"Counting words{' and tokens' if with_tokens else ''} in texts from books"):
        with open(file, "r") as f:
            total_words += reduce(lambda x, y: x + y, [len(pattern.findall(line)) for line in f.readlines()])
            for line in f.readlines():
                total_words += len(pattern.findall(line, re.UNICODE))
                if with_tokens:
                    total_tokens += len(encoding.encode(line, disallowed_special=()))
    return total_words, (total_tokens if with_tokens else None)


def _count_words_in_crawled_docs(with_tokens):
    total_words = 0
    total_tokens = 0
    pattern = re.compile(WORD_REGEX)
    with Progress() as progress:
        for repo_id in CRAWLED_DOCS_REPO_IDS:
            snapshot = snapshot_download(repo_id=repo_id, allow_patterns=["*clean*.parquet"], repo_type="dataset")
            df = pd.read_parquet(snapshot, columns=COLUMNS)
            task = progress.add_task(
                f"Counting words{' and tokens' if with_tokens else ''} of crawled documents in `{repo_id}`",
                total=len(df) * len(COLUMNS)
            )
            for column in COLUMNS:
                for row in df[column]:
                    progress.update(task, advance=1)
                    if not row:
                        continue
                    total_words += len(pattern.findall(row, re.UNICODE))
                    if with_tokens:
                        total_tokens += len(encoding.encode(row, disallowed_special=()))
            progress.update(task, advance=1)
    return total_words, (total_tokens if with_tokens else None)
