"""Simple architecture boundary checks for source imports."""

from __future__ import annotations

import ast
from pathlib import Path

SRC = Path("src")

ALLOWED_UTILS_IMPORTERS = {
    Path("src/core/config.py"),
    Path("src/core/db.py"),
    Path("src/core/paths.py"),
    Path("src/core/security.py"),
    Path("src/core/state.py"),
    Path("src/core/upstream_meta.py"),
    Path("src/core/yadisk.py"),
}

ALLOWED_GEMINI_IMPORTERS = {
    Path("src/gemini.py"),
    Path("src/integrations/gemini.py"),
}

ALLOWED_S3_IMPORTERS = {
    Path("src/s3.py"),
    Path("src/integrations/s3.py"),
}


def _imported_modules(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                modules.add(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                modules.add(node.module)
    return modules


def main() -> int:
    errors: list[str] = []
    for path in SRC.rglob("*.py"):
        modules = _imported_modules(path)

        if ("utils" in modules or any(m.startswith("utils.") for m in modules)) and path not in ALLOWED_UTILS_IMPORTERS:
            errors.append(f"{path}: direct utils import is forbidden outside core/*")

        if ("gemini" in modules or any(m.startswith("gemini.") for m in modules)) and path not in ALLOWED_GEMINI_IMPORTERS:
            errors.append(f"{path}: import integrations.gemini instead of gemini")

        if ("s3" in modules or any(m.startswith("s3.") for m in modules)) and path not in ALLOWED_S3_IMPORTERS:
            errors.append(f"{path}: import integrations.s3 instead of s3")

        if "meta_fields" in modules or any(m.startswith("meta_fields.") for m in modules):
            errors.append(f"{path}: meta_fields module is deprecated, use metadata.fields")

        if "meta" in modules or any(m.startswith("meta.") for m in modules):
            errors.append(f"{path}: meta package is deprecated, use metadata package")

    if errors:
        print("Architecture check failed:")
        for err in sorted(errors):
            print(f"- {err}")
        return 1

    print("Architecture check passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
