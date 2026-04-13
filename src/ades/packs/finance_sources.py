"""Finance-source snapshot fetchers for real-content pack generation."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from hashlib import sha256
import json
from pathlib import Path
import shutil
import urllib.request
from urllib.parse import urlparse

from .fetch import normalize_source_url


DEFAULT_FINANCE_SOURCE_OUTPUT_ROOT = Path(
    "/mnt/githubActions/ades_big_data/pack_sources/raw/finance-en"
)
DEFAULT_SEC_COMPANIES_URL = "https://www.sec.gov/files/company_tickers.json"
DEFAULT_SEC_SUBMISSIONS_URL = (
    "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip"
)
DEFAULT_SEC_COMPANYFACTS_URL = (
    "https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip"
)
DEFAULT_SYMBOL_DIRECTORY_URL = (
    "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt"
)
DEFAULT_OTHER_LISTED_URL = (
    "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"
)
DEFAULT_SOURCE_FETCH_USER_AGENT = "ades/0.1.0 (ops@adestool.com)"
DEFAULT_CURATED_FINANCE_ENTITIES: tuple[dict[str, object], ...] = ()


@dataclass(frozen=True)
class FinanceSourceFetchResult:
    """Summary of one downloaded finance source snapshot set."""

    pack_id: str
    output_dir: str
    snapshot: str
    snapshot_dir: str
    sec_companies_url: str
    sec_submissions_url: str
    sec_companyfacts_url: str
    symbol_directory_url: str
    other_listed_url: str
    source_manifest_path: str
    sec_companies_path: str
    sec_submissions_path: str
    sec_companyfacts_path: str
    symbol_directory_path: str
    other_listed_path: str
    curated_entities_path: str
    generated_at: str
    source_count: int
    curated_entity_count: int
    sec_companies_sha256: str
    sec_submissions_sha256: str
    sec_companyfacts_sha256: str
    symbol_directory_sha256: str
    other_listed_sha256: str
    curated_entities_sha256: str
    warnings: list[str] = field(default_factory=list)


def fetch_finance_source_snapshot(
    *,
    output_dir: str | Path = DEFAULT_FINANCE_SOURCE_OUTPUT_ROOT,
    snapshot: str | None = None,
    sec_companies_url: str = DEFAULT_SEC_COMPANIES_URL,
    sec_submissions_url: str = DEFAULT_SEC_SUBMISSIONS_URL,
    sec_companyfacts_url: str = DEFAULT_SEC_COMPANYFACTS_URL,
    symbol_directory_url: str = DEFAULT_SYMBOL_DIRECTORY_URL,
    other_listed_url: str = DEFAULT_OTHER_LISTED_URL,
    user_agent: str = DEFAULT_SOURCE_FETCH_USER_AGENT,
) -> FinanceSourceFetchResult:
    """Download one immutable finance source snapshot set into the big-data root."""

    resolved_output_dir = Path(output_dir).expanduser().resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    resolved_snapshot = _resolve_snapshot(snapshot)
    snapshot_dir = resolved_output_dir / resolved_snapshot
    if snapshot_dir.exists():
        raise FileExistsError(
            f"Finance source snapshot directory already exists: {snapshot_dir}"
        )
    snapshot_dir.mkdir(parents=True, exist_ok=False)

    generated_at = _utc_timestamp()
    warnings: list[str] = []
    try:
        sec_url = _download_source(
            sec_companies_url,
            snapshot_dir / "sec_company_tickers.json",
            user_agent=user_agent,
        )
        sec_submissions_resolved_url = _download_source(
            sec_submissions_url,
            snapshot_dir / "sec_submissions.zip",
            user_agent=user_agent,
        )
        sec_companyfacts_resolved_url = _download_source(
            sec_companyfacts_url,
            snapshot_dir / "sec_companyfacts.zip",
            user_agent=user_agent,
        )
        symbol_url = _download_source(
            symbol_directory_url,
            snapshot_dir / "nasdaqtraded.txt",
            user_agent=user_agent,
        )
        other_listed_resolved_url = _download_source(
            other_listed_url,
            snapshot_dir / "otherlisted.txt",
            user_agent=user_agent,
        )
        curated_entities_path = snapshot_dir / "curated_finance_entities.json"
        _write_curated_entities(curated_entities_path)
        _validate_sec_snapshot(snapshot_dir / "sec_company_tickers.json")
        _validate_nonempty_snapshot(snapshot_dir / "sec_submissions.zip")
        _validate_nonempty_snapshot(snapshot_dir / "sec_companyfacts.zip")
        _validate_symbol_directory(snapshot_dir / "nasdaqtraded.txt")
        _validate_symbol_directory(snapshot_dir / "otherlisted.txt")
    except Exception:
        shutil.rmtree(snapshot_dir, ignore_errors=True)
        raise

    sec_companies_path = snapshot_dir / "sec_company_tickers.json"
    sec_submissions_path = snapshot_dir / "sec_submissions.zip"
    sec_companyfacts_path = snapshot_dir / "sec_companyfacts.zip"
    symbol_directory_path = snapshot_dir / "nasdaqtraded.txt"
    other_listed_path = snapshot_dir / "otherlisted.txt"
    curated_entities_path = snapshot_dir / "curated_finance_entities.json"
    manifest_path = snapshot_dir / "sources.fetch.json"
    _write_source_manifest(
        path=manifest_path,
        snapshot=resolved_snapshot,
        generated_at=generated_at,
        user_agent=user_agent,
        sec_companies_url=sec_url,
        sec_submissions_url=sec_submissions_resolved_url,
        sec_companyfacts_url=sec_companyfacts_resolved_url,
        symbol_directory_url=symbol_url,
        other_listed_url=other_listed_resolved_url,
        sec_companies_path=sec_companies_path,
        sec_submissions_path=sec_submissions_path,
        sec_companyfacts_path=sec_companyfacts_path,
        symbol_directory_path=symbol_directory_path,
        other_listed_path=other_listed_path,
        curated_entities_path=curated_entities_path,
    )

    return FinanceSourceFetchResult(
        pack_id="finance-en",
        output_dir=str(resolved_output_dir),
        snapshot=resolved_snapshot,
        snapshot_dir=str(snapshot_dir),
        sec_companies_url=sec_url,
        sec_submissions_url=sec_submissions_resolved_url,
        sec_companyfacts_url=sec_companyfacts_resolved_url,
        symbol_directory_url=symbol_url,
        other_listed_url=other_listed_resolved_url,
        source_manifest_path=str(manifest_path),
        sec_companies_path=str(sec_companies_path),
        sec_submissions_path=str(sec_submissions_path),
        sec_companyfacts_path=str(sec_companyfacts_path),
        symbol_directory_path=str(symbol_directory_path),
        other_listed_path=str(other_listed_path),
        curated_entities_path=str(curated_entities_path),
        generated_at=generated_at,
        source_count=6,
        curated_entity_count=len(DEFAULT_CURATED_FINANCE_ENTITIES),
        sec_companies_sha256=_sha256_file(sec_companies_path),
        sec_submissions_sha256=_sha256_file(sec_submissions_path),
        sec_companyfacts_sha256=_sha256_file(sec_companyfacts_path),
        symbol_directory_sha256=_sha256_file(symbol_directory_path),
        other_listed_sha256=_sha256_file(other_listed_path),
        curated_entities_sha256=_sha256_file(curated_entities_path),
        warnings=warnings,
    )


def _resolve_snapshot(snapshot: str | None) -> str:
    if snapshot is None:
        return date.today().isoformat()
    return date.fromisoformat(snapshot).isoformat()


def _download_source(source_url: str | Path, destination: Path, *, user_agent: str) -> str:
    normalized_source_url = normalize_source_url(source_url)
    parsed = urlparse(normalized_source_url)
    request: str | urllib.request.Request
    if parsed.scheme in {"http", "https"}:
        request = urllib.request.Request(
            normalized_source_url,
            headers={
                "User-Agent": user_agent,
                "Accept": "application/json,text/plain;q=0.9,*/*;q=0.1",
            },
        )
    else:
        request = normalized_source_url
    with urllib.request.urlopen(request, timeout=60.0) as response:
        destination.write_bytes(response.read())
    return normalized_source_url


def _write_curated_entities(path: Path) -> None:
    payload = {"entities": list(DEFAULT_CURATED_FINANCE_ENTITIES)}
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _write_source_manifest(
    *,
    path: Path,
    snapshot: str,
    generated_at: str,
    user_agent: str,
    sec_companies_url: str,
    sec_submissions_url: str,
    sec_companyfacts_url: str,
    symbol_directory_url: str,
    other_listed_url: str,
    sec_companies_path: Path,
    sec_submissions_path: Path,
    sec_companyfacts_path: Path,
    symbol_directory_path: Path,
    other_listed_path: Path,
    curated_entities_path: Path,
) -> None:
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "pack_id": "finance-en",
                "snapshot": snapshot,
                "generated_at": generated_at,
                "user_agent": user_agent,
                "sources": [
                    _source_manifest_entry(
                        name="sec-company-tickers",
                        source_url=sec_companies_url,
                        path=sec_companies_path,
                    ),
                    _source_manifest_entry(
                        name="sec-submissions",
                        source_url=sec_submissions_url,
                        path=sec_submissions_path,
                    ),
                    _source_manifest_entry(
                        name="sec-companyfacts",
                        source_url=sec_companyfacts_url,
                        path=sec_companyfacts_path,
                    ),
                    _source_manifest_entry(
                        name="symbol-directory",
                        source_url=symbol_directory_url,
                        path=symbol_directory_path,
                    ),
                    _source_manifest_entry(
                        name="other-listed",
                        source_url=other_listed_url,
                        path=other_listed_path,
                    ),
                    _source_manifest_entry(
                        name="curated-finance-entities",
                        source_url="operator://ades/curated-finance-entities",
                        path=curated_entities_path,
                    ),
                ],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def _source_manifest_entry(*, name: str, source_url: str, path: Path) -> dict[str, object]:
    return {
        "name": name,
        "source_url": source_url,
        "path": path.name,
        "sha256": _sha256_file(path),
        "size_bytes": path.stat().st_size,
    }


def _validate_sec_snapshot(path: Path) -> None:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, (dict, list)):
        raise ValueError(f"Unsupported SEC snapshot structure: {path}")


def _validate_nonempty_snapshot(path: Path) -> None:
    if path.stat().st_size <= 0:
        raise ValueError(f"Snapshot is empty: {path}")


def _validate_symbol_directory(path: Path) -> None:
    lines = path.read_text(encoding="utf-8").splitlines()
    if not lines:
        raise ValueError(f"Symbol directory snapshot is empty: {path}")
    header = lines[0]
    if "Symbol" not in header:
        raise ValueError(f"Symbol directory snapshot header is invalid: {path}")


def _sha256_file(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _utc_timestamp() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )
