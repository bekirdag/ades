"""Fetch helpers for file and HTTP registry sources."""

from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse


def normalize_source_url(source: str | Path) -> str:
    """Normalize a path-like or URL-like source to a URL string."""

    if isinstance(source, Path):
        return source.expanduser().resolve().as_uri()

    parsed = urlparse(source)
    if parsed.scheme:
        return source
    return Path(source).expanduser().resolve().as_uri()


def read_bytes(source_url: str) -> bytes:
    """Read bytes from a file or HTTP(S) URL."""

    parsed = urlparse(source_url)
    if parsed.scheme == "file":
        return Path(parsed.path).read_bytes()
    if parsed.scheme in {"http", "https"}:
        import httpx

        response = httpx.get(source_url, timeout=30.0)
        response.raise_for_status()
        return response.content
    raise ValueError(f"Unsupported source URL: {source_url}")


def read_text(source_url: str) -> str:
    """Read UTF-8 text from a file or HTTP(S) URL."""

    return read_bytes(source_url).decode("utf-8")


def read_content_length(source_url: str) -> int | None:
    """Return a source size without downloading the full payload when possible."""

    parsed = urlparse(source_url)
    if parsed.scheme == "file":
        path = Path(parsed.path)
        return path.stat().st_size if path.exists() else None
    if parsed.scheme in {"http", "https"}:
        import httpx

        try:
            response = httpx.head(source_url, follow_redirects=True, timeout=10.0)
        except httpx.HTTPError:
            return None
        if response.status_code < 200 or response.status_code >= 400:
            return None
        value = response.headers.get("content-length")
        if value is None:
            return None
        try:
            return int(value)
        except ValueError:
            return None
    return None
