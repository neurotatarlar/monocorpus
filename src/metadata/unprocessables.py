"""Thread-safe persistence for metadata extraction failures."""

from __future__ import annotations

import os
import time


def add_unprocessable(md5: str, lock="unprocessables/unprocessables_meta.lock", file="unprocessables/unprocessables_meta.txt") -> None:
    """Append md5 to unprocessables file under a coarse lock file."""
    while os.path.exists(lock):
        time.sleep(1)
    try:
        with open(lock, "w"):
            unprocessables = {md5}
            if os.path.exists(file):
                with open(file, "r") as f:
                    for line in f:
                        unprocessables.add(line.strip())
            with open(file, "w") as f:
                for item in sorted(unprocessables):
                    f.write(f"{item}\n")
                f.flush()
    finally:
        if os.path.exists(lock):
            os.remove(lock)


def load_unprocessables(file="unprocessables/unprocessables_meta.txt") -> set[str]:
    """Load known unprocessable md5s from disk."""
    unprocessables = set()
    if os.path.exists(file):
        with open(file, "r") as f:
            for line in f:
                unprocessables.add(line.strip())
    return unprocessables
