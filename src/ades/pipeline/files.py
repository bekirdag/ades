"""Local file-loading helpers for tag requests."""

from __future__ import annotations

from pathlib import Path

HTML_SUFFIXES = {".htm", ".html", ".xhtml"}


def infer_content_type(path: str | Path) -> str:
    """Infer the supported content type for a local file path."""

    suffix = Path(path).suffix.casefold()
    if suffix in HTML_SUFFIXES:
        return "text/html"
    return "text/plain"


def load_tag_file(
    path: str | Path,
    *,
    content_type: str | None = None,
) -> tuple[Path, str, str]:
    """Resolve, validate, and read a local file for tagging."""

    resolved_path = Path(path).expanduser().resolve()
    if not resolved_path.exists():
        raise FileNotFoundError(f"File not found: {resolved_path}")
    if not resolved_path.is_file():
        raise IsADirectoryError(f"Path is not a file: {resolved_path}")

    resolved_content_type = content_type or infer_content_type(resolved_path)
    text = resolved_path.read_text(encoding="utf-8", errors="replace")
    return resolved_path, text, resolved_content_type


def load_tag_files(
    paths: list[str | Path] | tuple[str | Path, ...],
    *,
    content_type: str | None = None,
) -> list[tuple[Path, str, str]]:
    """Resolve, validate, and read multiple local files for tagging."""

    if not paths:
        raise ValueError("At least one file path is required.")
    return [load_tag_file(path, content_type=content_type) for path in paths]
