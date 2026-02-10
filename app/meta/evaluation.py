"""Compatibility wrapper that re-exports meta evaluation from src."""

from __future__ import annotations

import os
import sys


SRC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src"))
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from meta.evaluation import *  # noqa: E402,F401,F403
