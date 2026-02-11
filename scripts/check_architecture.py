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

FORBIDDEN_ROOT_SHIMS = {
    "gemini",
    "s3",
    "db",
    "prompt",
    "prepare_shots",
    "check_pub_links",
    "dump_state",
    "match_limited",
    "sharing_restricted",
}


def _imported_modules(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                modules.add(alias.name)
        elif isinstance(node, ast.ImportFrom):
            # Skip relative imports; only enforce boundaries on absolute imports.
            if node.module and node.level == 0:
                modules.add(node.module)
    return modules


def main() -> int:
    errors: list[str] = []
    for path in SRC.rglob("*.py"):
        modules = _imported_modules(path)

        if ("utils" in modules or any(m.startswith("utils.") for m in modules)) and path not in ALLOWED_UTILS_IMPORTERS:
            errors.append(f"{path}: direct utils import is forbidden outside core/*")

        for module in FORBIDDEN_ROOT_SHIMS:
            if module in modules or any(m.startswith(f"{module}.") for m in modules):
                errors.append(f"{path}: root module '{module}' is deprecated; import package variant")

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
