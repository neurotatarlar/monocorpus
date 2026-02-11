"""Pure text normalization helpers used by pps postprocessing."""

from typing import Dict, List, Tuple
import re

UNDERSCORE_CHARS = set("_＿﹍﹎")
INLINE_UNDERSCORE_FIX_RE = re.compile(r"(\\\\{2,})([_＿﹍﹎]{10,})|(?<!\\\\)([_＿﹍﹎]{11,})")
FENCE_RE = re.compile(r"^(?P<fence>`{3,}|~{3,})")
CYRILLIC_RE = re.compile(r"[\u0400-\u04FF]")
WORD_RE = re.compile(r"\w+", flags=re.UNICODE)
DISPLAY_MATH_RE = re.compile(r"(?<!\\)\$\$(.+?)(?<!\\)\$\$", flags=re.DOTALL)
INLINE_MATH_RE = re.compile(r"(?<!\\)\$(?!\$)([^\n]*?)(?<!\\)\$(?!\$)")

LOOKALIKE_TO_TATAR: Dict[str, str] = {}
for _chars, _target in (
    ("ə", "ә"),
    ("Ə", "Ә"),
    ("eėĕẹéèêëēěęẽẻȅȇếềểễệ", "е"),
    ("EĖĔẸÉÈÊËĒĚĘẼẺȄȆẾỀỂỄỆ", "Е"),
    ("oọőôöòóõøōŏȯȱỏốồổỗộ", "о"),
    ("OỌŐÔÖÒÓÕØŌŎȮȰỎỐỒỔỖỘ", "О"),
    ("aàáâãäåāăąȁȃȧȩǎǟǡǻ", "а"),
    ("AÀÁÂÃÄÅĀĂĄȀȂȦȨǍǞǠǺ", "А"),
    ("yýỳỹỷỵȳÿ", "у"),
    ("YÝỲỸỶỴȲŸ", "У"),
    ("cƈċ", "с"),
    ("CƇĊ", "С"),
    ("xẋẍҳӽӿ", "х"),
    ("XẊẌҲӼӾ", "Х"),
    ("pṗṕ", "р"),
    ("PṖṔ", "Р"),
    ("hħḥḧḩḫḣ", "һ"),
    ("HĦḤḦḨḪḢ", "Һ"),
    ("kķĸқҡҟҝќ", "к"),
    ("KĶҚҠҞҜЌ", "К"),
    ("ґғ", "г"),
    ("ҐҒ", "Г"),
):
    for _ch in _chars:
        LOOKALIKE_TO_TATAR[_ch] = _target


def truncate_underscore_runs(text: str, limit: int = 10) -> Tuple[str, int]:
    """Limit long underscore runs and normalize escaping for inline placeholders."""
    removed = 0
    out_lines = []

    for line in text.splitlines(True):
        content = line.rstrip("\r\n")
        newline = line[len(content):]
        stripped = content.strip()

        if stripped and all(ch in UNDERSCORE_CHARS for ch in stripped):
            underscore_count = sum(1 for ch in stripped if ch in UNDERSCORE_CHARS)
            if underscore_count > limit:
                removed += underscore_count - limit
            prefix = content[: len(content) - len(content.lstrip())]
            suffix = content[len(content.rstrip()):]
            out_lines.append(f"{prefix}{'_' * limit}{suffix}{newline}")
            continue

        def _inline_repl(match: re.Match) -> str:
            nonlocal removed
            backslashes = match.group(1)
            if backslashes:
                run = match.group(2)
                if len(backslashes) > 1:
                    removed += len(backslashes) - 1
                if len(run) > limit:
                    removed += len(run) - limit
                    run = "_" * limit
                return f"\\{run}"
            run = match.group(3)
            removed += len(run) - limit
            return f"\\_{'_' * (limit - 1)}"

        content = INLINE_UNDERSCORE_FIX_RE.sub(_inline_repl, content)
        out_lines.append(f"{content}{newline}")

    return "".join(out_lines), removed


def normalize_mixed_script_lookalikes(text: str) -> Tuple[str, int]:
    """Normalize Latin/other-script lookalikes inside Cyrillic words."""
    replaced = 0
    out_lines = []
    in_fence = False
    fence_char = None
    fence_len = 0

    for line in text.splitlines(True):
        fence_match = FENCE_RE.match(line)
        if fence_match:
            fence = fence_match.group("fence")
            if not in_fence:
                in_fence = True
                fence_char = fence[0]
                fence_len = len(fence)
            else:
                if fence_char and line.startswith(fence_char * fence_len):
                    in_fence = False
                    fence_char = None
                    fence_len = 0
            out_lines.append(line)
            continue

        if in_fence:
            out_lines.append(line)
            continue

        normalized, line_replaced = _normalize_line_outside_inline_code(line)
        replaced += line_replaced
        out_lines.append(normalized)

    return "".join(out_lines), replaced


def mask_math_segments(text: str) -> Tuple[str, Dict[str, str]]:
    """Replace math segments with placeholders so markdown formatter won't alter formulas."""
    placeholders: Dict[str, str] = {}

    def _new_placeholder() -> str:
        return f"MATHPLACEHOLDER{len(placeholders):08d}TOKEN"

    def _display_repl(match: re.Match) -> str:
        key = _new_placeholder()
        placeholders[key] = match.group(0)
        return key

    def _inline_repl(match: re.Match) -> str:
        key = _new_placeholder()
        placeholders[key] = match.group(0)
        return key

    masked = DISPLAY_MATH_RE.sub(_display_repl, text)
    masked = INLINE_MATH_RE.sub(_inline_repl, masked)
    return masked, placeholders


def restore_math_segments(text: str, placeholders: Dict[str, str]) -> str:
    """Restore formulas previously replaced by placeholders."""
    for key, value in placeholders.items():
        text = text.replace(key, value)
    return text


def _normalize_line_outside_inline_code(line: str) -> Tuple[str, int]:
    replaced = 0
    parts: List[str] = []
    i = 0
    n = len(line)

    while i < n:
        backtick = line.find("`", i)
        if backtick == -1:
            segment, seg_replaced = _normalize_mixed_tokens(line[i:])
            parts.append(segment)
            replaced += seg_replaced
            break

        segment, seg_replaced = _normalize_mixed_tokens(line[i:backtick])
        parts.append(segment)
        replaced += seg_replaced

        j = backtick
        while j < n and line[j] == "`":
            j += 1
        delim = line[backtick:j]
        end = line.find(delim, j)
        if end == -1:
            parts.append(delim)
            i = j
            continue

        parts.append(line[backtick : end + len(delim)])
        i = end + len(delim)

    return "".join(parts), replaced


def _normalize_mixed_tokens(segment: str) -> Tuple[str, int]:
    replaced = 0

    def _repl(match: re.Match) -> str:
        nonlocal replaced
        token = match.group(0)
        if not CYRILLIC_RE.search(token):
            return token
        changed = False
        out = []
        for ch in token:
            mapped = LOOKALIKE_TO_TATAR.get(ch, ch)
            if mapped != ch:
                replaced += 1
                changed = True
            out.append(mapped)
        if not changed:
            return token
        return "".join(out)

    return WORD_RE.sub(_repl, segment), replaced
