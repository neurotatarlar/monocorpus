"""Compatibility entrypoint that delegates to the consolidated src CLI."""

from __future__ import annotations

import os
import sys


SRC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from cli import app  # noqa: E402

if __name__ == "__main__":
    app()
