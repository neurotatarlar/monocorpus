"""Compatibility wrapper that re-exports ORM models from src."""

from __future__ import annotations

import os
import sys


SRC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src"))
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from models import Base, Document, DocumentCrh  # noqa: E402,F401
