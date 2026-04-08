"""Local file-loading helpers for tag requests."""

from __future__ import annotations

from dataclasses import asdict
from dataclasses import dataclass
from fnmatch import fnmatch
from glob import glob
from pathlib import Path
from typing import Iterable

HTML_SUFFIXES = {".htm", ".html", ".xhtml"}
GENERATED_OUTPUT_SUFFIX = ".ades.json"


@dataclass(slots=True)
class TagFileDiscoverySummary:
    """Machine-readable summary for a batch file discovery run."""

    explicit_path_count: int = 0
    directory_match_count: int = 0
    glob_match_count: int = 0
    discovered_count: int = 0
    included_count: int = 0
    excluded_count: int = 0
    duplicate_count: int = 0
    generated_output_skipped_count: int = 0
    recursive: bool = True
    include_patterns: tuple[str, ...] = ()
    exclude_patterns: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        """Return a stable dictionary payload for API model construction."""

        payload = asdict(self)
        payload["include_patterns"] = list(self.include_patterns)
        payload["exclude_patterns"] = list(self.exclude_patterns)
        return payload


@dataclass(slots=True)
class TagFileDiscoveryResult:
    """Resolved paths plus summary metadata for a batch discovery run."""

    paths: list[Path]
    summary: TagFileDiscoverySummary


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


def _matches_patterns(path: Path, patterns: tuple[str, ...]) -> bool:
    """Return whether a path matches any user-provided glob filter."""

    normalized_path = path.as_posix()
    basename = path.name
    return any(fnmatch(normalized_path, pattern) or fnmatch(basename, pattern) for pattern in patterns)


def discover_tag_file_sources(
    *,
    paths: Iterable[str | Path] = (),
    directories: Iterable[str | Path] = (),
    glob_patterns: Iterable[str] = (),
    recursive: bool = True,
    include_patterns: Iterable[str] = (),
    exclude_patterns: Iterable[str] = (),
) -> TagFileDiscoveryResult:
    """Resolve explicit files, directories, and globs into a filtered discovery result."""

    resolved_paths: list[Path] = []
    seen: set[Path] = set()
    normalized_include_patterns = tuple(pattern for pattern in include_patterns if pattern)
    normalized_exclude_patterns = tuple(pattern for pattern in exclude_patterns if pattern)
    summary = TagFileDiscoverySummary(
        recursive=recursive,
        include_patterns=normalized_include_patterns,
        exclude_patterns=normalized_exclude_patterns,
    )

    def add_candidate(
        candidate: Path,
        *,
        allow_generated_output: bool,
        source_kind: str,
    ) -> None:
        resolved_candidate = candidate.expanduser().resolve()
        if not allow_generated_output and resolved_candidate.name.endswith(GENERATED_OUTPUT_SUFFIX):
            summary.generated_output_skipped_count += 1
            return
        if source_kind == "explicit":
            summary.explicit_path_count += 1
        elif source_kind == "directory":
            summary.directory_match_count += 1
        elif source_kind == "glob":
            summary.glob_match_count += 1
        if resolved_candidate in seen:
            summary.duplicate_count += 1
            return
        seen.add(resolved_candidate)
        resolved_paths.append(resolved_candidate)

    for path in paths:
        add_candidate(
            resolve_tag_file(path),
            allow_generated_output=True,
            source_kind="explicit",
        )

    for directory in directories:
        resolved_directory = Path(directory).expanduser().resolve()
        if not resolved_directory.exists():
            raise FileNotFoundError(f"Directory not found: {resolved_directory}")
        if not resolved_directory.is_dir():
            raise NotADirectoryError(f"Path is not a directory: {resolved_directory}")
        iterator = resolved_directory.rglob("*") if recursive else resolved_directory.glob("*")
        for candidate in sorted(path for path in iterator if path.is_file()):
            add_candidate(
                candidate,
                allow_generated_output=False,
                source_kind="directory",
            )

    for pattern in glob_patterns:
        for candidate in sorted(Path(match) for match in glob(pattern, recursive=True)):
            if candidate.is_file():
                add_candidate(
                    candidate,
                    allow_generated_output=False,
                    source_kind="glob",
                )

    if not resolved_paths:
        raise FileNotFoundError("No input files matched the provided file paths, directories, or globs.")
    summary.discovered_count = len(resolved_paths)

    filtered_paths = [
        path
        for path in resolved_paths
        if (
            (not normalized_include_patterns or _matches_patterns(path, normalized_include_patterns))
            and not _matches_patterns(path, normalized_exclude_patterns)
        )
    ]
    summary.included_count = len(filtered_paths)
    summary.excluded_count = summary.discovered_count - summary.included_count

    if not filtered_paths:
        raise FileNotFoundError("No input files remained after applying include/exclude filters.")

    return TagFileDiscoveryResult(paths=filtered_paths, summary=summary)


def resolve_tag_file_sources(
    *,
    paths: Iterable[str | Path] = (),
    directories: Iterable[str | Path] = (),
    glob_patterns: Iterable[str] = (),
    recursive: bool = True,
    include_patterns: Iterable[str] = (),
    exclude_patterns: Iterable[str] = (),
) -> list[Path]:
    """Resolve explicit files, directories, and globs into a unique ordered file list."""

    return discover_tag_file_sources(
        paths=paths,
        directories=directories,
        glob_patterns=glob_patterns,
        recursive=recursive,
        include_patterns=include_patterns,
        exclude_patterns=exclude_patterns,
    ).paths
