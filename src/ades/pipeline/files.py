"""Local file-loading helpers for tag requests."""

from __future__ import annotations

from dataclasses import asdict
from dataclasses import dataclass
from fnmatch import fnmatch
from glob import glob
import hashlib
from pathlib import Path
from typing import Iterable

HTML_SUFFIXES = {".htm", ".html", ".xhtml"}
GENERATED_OUTPUT_SUFFIX = ".ades.json"


@dataclass(slots=True)
class TagFileSkippedEntry:
    """One skipped corpus input with a deterministic reason."""

    reference: str
    reason: str
    source_kind: str
    size_bytes: int | None = None

    def to_dict(self) -> dict[str, object]:
        """Return a stable dictionary payload for API model construction."""

        return asdict(self)


@dataclass(slots=True)
class TagSourceFingerprint:
    """Stable local fingerprint for a source file."""

    size_bytes: int
    modified_time_ns: int
    sha256: str

    def to_dict(self) -> dict[str, object]:
        """Return a stable dictionary payload for API model construction."""

        return asdict(self)


@dataclass(slots=True)
class TagFileRejectedEntry:
    """One rejected corpus input that could not be accepted for discovery."""

    reference: str
    reason: str
    source_kind: str

    def to_dict(self) -> dict[str, str]:
        """Return a stable dictionary payload for API model construction."""

        return asdict(self)


@dataclass(slots=True)
class TagFileDiscoverySummary:
    """Machine-readable summary for a batch file discovery run."""

    explicit_path_count: int = 0
    directory_match_count: int = 0
    glob_match_count: int = 0
    discovered_count: int = 0
    included_count: int = 0
    processed_count: int = 0
    excluded_count: int = 0
    skipped_count: int = 0
    rejected_count: int = 0
    limit_skipped_count: int = 0
    unchanged_skipped_count: int = 0
    duplicate_count: int = 0
    generated_output_skipped_count: int = 0
    discovered_input_bytes: int = 0
    included_input_bytes: int = 0
    processed_input_bytes: int = 0
    max_files: int | None = None
    max_input_bytes: int | None = None
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
    skipped: list[TagFileSkippedEntry]
    rejected: list[TagFileRejectedEntry]


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
) -> tuple[Path, str, str, int, TagSourceFingerprint]:
    """Resolve, validate, and read a local file for tagging."""

    resolved_path = resolve_tag_file(path)
    resolved_content_type = content_type or infer_content_type(resolved_path)
    payload = resolved_path.read_bytes()
    stat = resolved_path.stat()
    text = payload.decode("utf-8", errors="replace")
    fingerprint = TagSourceFingerprint(
        size_bytes=stat.st_size,
        modified_time_ns=stat.st_mtime_ns,
        sha256=hashlib.sha256(payload).hexdigest(),
    )
    return resolved_path, text, resolved_content_type, stat.st_size, fingerprint


def load_tag_files(
    paths: list[str | Path] | tuple[str | Path, ...],
    *,
    content_type: str | None = None,
) -> list[tuple[Path, str, str, int, TagSourceFingerprint]]:
    """Resolve, validate, and read multiple local files for tagging."""

    if not paths:
        return []
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
    max_files: int | None = None,
    max_input_bytes: int | None = None,
) -> TagFileDiscoveryResult:
    """Resolve explicit files, directories, and globs into a filtered discovery result."""

    if max_files is not None and max_files < 0:
        raise ValueError("max_files must be zero or greater.")
    if max_input_bytes is not None and max_input_bytes < 0:
        raise ValueError("max_input_bytes must be zero or greater.")

    resolved_paths: list[Path] = []
    path_sizes: dict[Path, int] = {}
    seen: set[Path] = set()
    skipped: list[TagFileSkippedEntry] = []
    rejected: list[TagFileRejectedEntry] = []
    normalized_include_patterns = tuple(pattern for pattern in include_patterns if pattern)
    normalized_exclude_patterns = tuple(pattern for pattern in exclude_patterns if pattern)
    summary = TagFileDiscoverySummary(
        max_files=max_files,
        max_input_bytes=max_input_bytes,
        recursive=recursive,
        include_patterns=normalized_include_patterns,
        exclude_patterns=normalized_exclude_patterns,
    )

    def size_for(candidate: Path) -> int | None:
        try:
            return candidate.stat().st_size
        except OSError:
            return None

    def add_skipped(
        reference: str | Path,
        *,
        reason: str,
        source_kind: str,
        size_bytes: int | None = None,
    ) -> None:
        skipped.append(
            TagFileSkippedEntry(
                reference=str(reference),
                reason=reason,
                source_kind=source_kind,
                size_bytes=size_bytes,
            )
        )

    def add_rejected(reference: str | Path, *, reason: str, source_kind: str) -> None:
        rejected.append(
            TagFileRejectedEntry(
                reference=str(reference),
                reason=reason,
                source_kind=source_kind,
            )
        )

    def add_candidate(
        candidate: Path,
        *,
        allow_generated_output: bool,
        source_kind: str,
    ) -> None:
        resolved_candidate = candidate.expanduser().resolve()
        candidate_size = size_for(resolved_candidate)
        if not allow_generated_output and resolved_candidate.name.endswith(GENERATED_OUTPUT_SUFFIX):
            summary.generated_output_skipped_count += 1
            add_skipped(
                resolved_candidate,
                reason="generated_output",
                source_kind=source_kind,
                size_bytes=candidate_size,
            )
            return
        if source_kind == "explicit":
            summary.explicit_path_count += 1
        elif source_kind == "directory":
            summary.directory_match_count += 1
        elif source_kind == "glob":
            summary.glob_match_count += 1
        if resolved_candidate in seen:
            summary.duplicate_count += 1
            add_skipped(
                resolved_candidate,
                reason="duplicate",
                source_kind=source_kind,
                size_bytes=candidate_size,
            )
            return
        seen.add(resolved_candidate)
        resolved_paths.append(resolved_candidate)
        if candidate_size is not None:
            path_sizes[resolved_candidate] = candidate_size

    for path in paths:
        try:
            resolved_explicit_path = resolve_tag_file(path)
        except FileNotFoundError:
            add_rejected(Path(path).expanduser(), reason="file_not_found", source_kind="explicit")
            continue
        except IsADirectoryError:
            add_rejected(Path(path).expanduser(), reason="not_a_file", source_kind="explicit")
            continue
        add_candidate(
            resolved_explicit_path,
            allow_generated_output=True,
            source_kind="explicit",
        )

    for directory in directories:
        resolved_directory = Path(directory).expanduser().resolve()
        if not resolved_directory.exists():
            add_rejected(
                resolved_directory,
                reason="directory_not_found",
                source_kind="directory",
            )
            continue
        if not resolved_directory.is_dir():
            add_rejected(
                resolved_directory,
                reason="not_a_directory",
                source_kind="directory",
            )
            continue
        iterator = resolved_directory.rglob("*") if recursive else resolved_directory.glob("*")
        for candidate in sorted(path for path in iterator if path.is_file()):
            add_candidate(
                candidate,
                allow_generated_output=False,
                source_kind="directory",
            )

    for pattern in glob_patterns:
        matched_files = False
        for candidate in sorted(Path(match) for match in glob(pattern, recursive=True)):
            if candidate.is_file():
                matched_files = True
                add_candidate(
                    candidate,
                    allow_generated_output=False,
                    source_kind="glob",
                )
            else:
                add_skipped(candidate, reason="not_a_file_match", source_kind="glob")
        if not matched_files:
            add_skipped(pattern, reason="glob_no_matches", source_kind="glob")

    summary.discovered_count = len(resolved_paths)
    summary.discovered_input_bytes = sum(path_sizes.get(path, 0) for path in resolved_paths)

    filtered_paths: list[Path] = []
    for path in resolved_paths:
        path_size = path_sizes.get(path)
        if normalized_include_patterns and not _matches_patterns(path, normalized_include_patterns):
            add_skipped(
                path,
                reason="include_filter_miss",
                source_kind="filter",
                size_bytes=path_size,
            )
            continue
        if _matches_patterns(path, normalized_exclude_patterns):
            add_skipped(
                path,
                reason="exclude_filter_match",
                source_kind="filter",
                size_bytes=path_size,
            )
            continue
        filtered_paths.append(path)

    summary.included_count = len(filtered_paths)
    summary.included_input_bytes = sum(path_sizes.get(path, 0) for path in filtered_paths)
    summary.excluded_count = summary.discovered_count - summary.included_count

    limited_paths: list[Path] = []
    processed_input_bytes = 0
    file_limit_reached = False
    byte_limit_reached = False
    for path in filtered_paths:
        path_size = path_sizes.get(path)
        if file_limit_reached:
            summary.limit_skipped_count += 1
            add_skipped(
                path,
                reason="max_files_limit",
                source_kind="limit",
                size_bytes=path_size,
            )
            continue
        if byte_limit_reached:
            summary.limit_skipped_count += 1
            add_skipped(
                path,
                reason="max_input_bytes_limit",
                source_kind="limit",
                size_bytes=path_size,
            )
            continue
        if max_files is not None and len(limited_paths) >= max_files:
            file_limit_reached = True
            summary.limit_skipped_count += 1
            add_skipped(
                path,
                reason="max_files_limit",
                source_kind="limit",
                size_bytes=path_size,
            )
            continue
        if max_input_bytes is not None and processed_input_bytes + (path_size or 0) > max_input_bytes:
            byte_limit_reached = True
            summary.limit_skipped_count += 1
            add_skipped(
                path,
                reason="max_input_bytes_limit",
                source_kind="limit",
                size_bytes=path_size,
            )
            continue
        limited_paths.append(path)
        processed_input_bytes += path_size or 0

    summary.skipped_count = len(skipped)
    summary.rejected_count = len(rejected)
    summary.processed_count = len(limited_paths)
    summary.processed_input_bytes = processed_input_bytes

    return TagFileDiscoveryResult(
        paths=limited_paths,
        summary=summary,
        skipped=skipped,
        rejected=rejected,
    )


def resolve_tag_file_sources(
    *,
    paths: Iterable[str | Path] = (),
    directories: Iterable[str | Path] = (),
    glob_patterns: Iterable[str] = (),
    recursive: bool = True,
    include_patterns: Iterable[str] = (),
    exclude_patterns: Iterable[str] = (),
    max_files: int | None = None,
    max_input_bytes: int | None = None,
) -> list[Path]:
    """Resolve explicit files, directories, and globs into a unique ordered file list."""

    return discover_tag_file_sources(
        paths=paths,
        directories=directories,
        glob_patterns=glob_patterns,
        recursive=recursive,
        include_patterns=include_patterns,
        exclude_patterns=exclude_patterns,
        max_files=max_files,
        max_input_bytes=max_input_bytes,
    ).paths
