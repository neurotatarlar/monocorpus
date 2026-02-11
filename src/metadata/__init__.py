"""Metadata pipeline entrypoints: extraction and evaluation."""

from .dispatch import extract_metadata
from .evaluation import evaluate

__all__ = ["extract_metadata", "evaluate"]
