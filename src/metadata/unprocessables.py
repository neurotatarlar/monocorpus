"""Thread-safe persistence for metadata extraction failures."""

from __future__ import annotations

import os
import time

ARTIFACTS_DIR = "_artifacts"
UNPROCESSABLES_DIR = os.path.join(ARTIFACTS_DIR, "unprocessables")


def _legacy_path(path: str) -> str:
    """Return legacy root path for a preferred _artifacts path."""
    prefix = f"{ARTIFACTS_DIR}/"
    if path.startswith(prefix):
        return path.removeprefix(prefix)
    return path


def _ensure_parent_dir(path: str) -> None:
    """Create parent directory for a file path when needed."""
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def add_unprocessable(
    md5: str,
    lock=os.path.join(UNPROCESSABLES_DIR, "unprocessables_meta.lock"),
    file=os.path.join(UNPROCESSABLES_DIR, "unprocessables_meta.txt"),
) -> None:
    """Append md5 to unprocessables file under a coarse lock file."""
    _ensure_parent_dir(lock)
    _ensure_parent_dir(file)
    while os.path.exists(lock):
        time.sleep(1)
    try:
        with open(lock, "w"):
            unprocessables = {md5}
            for candidate in (file, _legacy_path(file)):
                if os.path.exists(candidate):
                    with open(candidate, "r") as f:
                        for line in f:
                            if line.strip():
                                unprocessables.add(line.strip())
            with open(file, "w") as f:
                for item in sorted(unprocessables):
                    f.write(f"{item}\n")
                f.flush()
    finally:
        if os.path.exists(lock):
            os.remove(lock)


def load_unprocessables(file=os.path.join(UNPROCESSABLES_DIR, "unprocessables_meta.txt")) -> set[str]:
    """Load known unprocessable md5s from disk."""
    unprocessables = set()
    for candidate in (file, _legacy_path(file)):
        if os.path.exists(candidate):
            with open(candidate, "r") as f:
                for line in f:
                    if line.strip():
                        unprocessables.add(line.strip())
    return unprocessables
