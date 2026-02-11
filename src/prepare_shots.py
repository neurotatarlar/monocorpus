"""Backward-compatible shim for prompts.shots."""

from prompts.shots import _form_inline_shots, _list_files, load_inline_shots

__all__ = ["load_inline_shots", "_form_inline_shots", "_list_files"]
