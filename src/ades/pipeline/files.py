"""Local file-loading helpers for tag requests."""

from __future__ import annotations

from glob import glob
from pathlib import Path
from typing import Iterable

HTML_SUFFIXES = {".htm", ".html", ".xhtml"}
GENERATED_OUTPUT_SUFFIX = ".ades.json"


def infer_content_type(path: str | Path) -> str:
    """Infer the supported content type for a local file path."""

    suffix = Path(path).suffix.casefold()
    if suffix in HTML_SUFFIXES:
        return "text/html"
    return "text/plain"


def resolve_tag_file(path: str | Path) -> Path:
    """Resolve and validate a single local file path for tagging."""

    resolved_path = Path(path).expanduser().resolve()
    if not resolved_path.exists():
        raise FileNotFoundError(f"File not found: {resolved_path}")
    if not resolved_path.is_file():
        raise IsADirectoryError(f"Path is not a file: {resolved_path}")
    return resolved_path


def load_tag_file(
    path: str | Path,
    *,
    content_type: str | None = None,
) -> tuple[Path, str, str]:
    """Resolve, validate, and read a local file for tagging."""

    resolved_path = resolve_tag_file(path)
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


def resolve_tag_file_sources(
    *,
    paths: Iterable[str | Path] = (),
    directories: Iterable[str | Path] = (),
    glob_patterns: Iterable[str] = (),
    recursive: bool = True,
) -> list[Path]:
    """Resolve explicit files, directories, and globs into a unique ordered file list."""

    resolved_paths: list[Path] = []
    seen: set[Path] = set()

    def add_candidate(candidate: Path, *, allow_generated_output: bool) -> None:
        resolved_candidate = candidate.expanduser().resolve()
        if not allow_generated_output and resolved_candidate.name.endswith(GENERATED_OUTPUT_SUFFIX):
            return
        if resolved_candidate in seen:
            return
        seen.add(resolved_candidate)
        resolved_paths.append(resolved_candidate)

    for path in paths:
        add_candidate(resolve_tag_file(path), allow_generated_output=True)

    for directory in directories:
        resolved_directory = Path(directory).expanduser().resolve()
        if not resolved_directory.exists():
            raise FileNotFoundError(f"Directory not found: {resolved_directory}")
        if not resolved_directory.is_dir():
            raise NotADirectoryError(f"Path is not a directory: {resolved_directory}")
        iterator = resolved_directory.rglob("*") if recursive else resolved_directory.glob("*")
        for candidate in sorted(path for path in iterator if path.is_file()):
            add_candidate(candidate, allow_generated_output=False)

    for pattern in glob_patterns:
        for candidate in sorted(Path(match) for match in glob(pattern, recursive=True)):
            if candidate.is_file():
                add_candidate(candidate, allow_generated_output=False)

    if not resolved_paths:
        raise FileNotFoundError("No input files matched the provided file paths, directories, or globs.")
    return resolved_paths
