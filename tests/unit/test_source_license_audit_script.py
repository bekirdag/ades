import csv
from datetime import date
import json
from pathlib import Path
import subprocess
import sys

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"


CANONICAL_EDGE_COLUMNS = [
    "source_ref",
    "source_type",
    "target_ref",
    "target_type",
    "relation",
    "jurisdiction",
    "source_url",
    "source_title",
    "source_snapshot",
    "source_year",
    "source_tier",
    "evidence",
    "effective_start_date",
    "effective_end_date",
    "confidence",
    "notes",
    "fetched_at",
    "content_hash",
    "review_status",
    "license_status",
    "license_notes",
    "confidence_basis",
    "refresh_policy",
]


def _write_canonical_edges(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, delimiter="\t", fieldnames=CANONICAL_EDGE_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow(_canonical_edge(row))


def _canonical_edge(overrides: dict[str, str]) -> dict[str, str]:
    row = {
        "source_ref": "ades:org:operating-alpha",
        "source_type": "organization",
        "target_ref": "ades:holding:alpha",
        "target_type": "holding_company",
        "relation": "org_part_of_holding",
        "jurisdiction": "GB",
        "source_url": "https://example.test/source",
        "source_title": "Example source",
        "source_snapshot": "2026-06-20",
        "source_year": "2026",
        "source_tier": "government",
        "evidence": "Official source backs the relationship.",
        "effective_start_date": "2026-01-01",
        "effective_end_date": "",
        "confidence": "0.9",
        "notes": "",
        "fetched_at": "2026-06-20T00:00:00Z",
        "content_hash": "sha256:abc123",
        "review_status": "accepted",
        "license_status": "allowed",
        "license_notes": "Official public source; normalized relationship metadata only.",
        "confidence_basis": "official source page",
        "refresh_policy": "quarterly",
    }
    row.update(overrides)
    return row


def _audit_source_licenses(**kwargs) -> dict[str, object]:
    if str(SRC_ROOT) not in sys.path:
        sys.path.insert(0, str(SRC_ROOT))
    from scripts.audit_source_licenses import audit_source_licenses

    return audit_source_licenses(**kwargs)


def test_quarterly_source_license_audit_flags_blocked_rows_and_manifests(
    tmp_path: Path,
) -> None:
    edge_path = tmp_path / "canonical_edges.tsv"
    manifest_path = tmp_path / "source_manifest.json"
    _write_canonical_edges(
        edge_path,
        [
            _canonical_edge({}),
            _canonical_edge(
                {
                    "source_ref": "ades:org:restricted",
                    "license_status": "restricted",
                    "license_notes": "Commercial terms block normalized output.",
                }
            ),
            _canonical_edge(
                {
                    "source_ref": "ades:org:missing-note",
                    "license_status": "allowed",
                    "license_notes": "",
                }
            ),
        ],
    )
    manifest_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "sources": [
                    {
                        "source_name": "Allowed manifest source",
                        "source_url": "https://example.test/allowed",
                        "normalized_output_allowed": True,
                        "license_notes": "Official public source; normalized metadata only.",
                    },
                    {
                        "source_name": "Unconfirmed manifest source",
                        "source_url": "https://example.test/unconfirmed",
                        "normalized_output_allowed": False,
                        "license_notes": "",
                    },
                ],
            }
        ),
        encoding="utf-8",
    )

    payload = _audit_source_licenses(
        edge_tsv_paths=[edge_path],
        source_manifest_paths=[manifest_path],
        as_of_date=date(2026, 7, 1),
        sample_limit=10,
    )

    assert payload["status"] == "needs_license_review"
    assert payload["summary"] == {
        "source_row_count": 3,
        "legacy_source_row_count": 0,
        "manifest_source_count": 2,
        "allowed_source_count": 2,
        "blocked_source_count": 3,
    }
    assert payload["license_warning_counts"] == {
        "missing_license_notes": 2,
        "source_license_restricted": 1,
        "source_use_not_confirmed": 1,
    }
    samples = {(sample["scope"], sample["warning"]) for sample in payload["license_warning_samples"]}
    assert ("source_row", "source_license_restricted") in samples
    assert ("source_manifest", "source_use_not_confirmed") in samples
    assert {item["category"] for item in payload["action_items"]} == {"source_license"}


def test_quarterly_source_license_audit_cli_emits_clean_json(tmp_path: Path) -> None:
    edge_path = tmp_path / "canonical_edges.tsv"
    manifest_path = tmp_path / "source_manifest.json"
    _write_canonical_edges(edge_path, [_canonical_edge({})])
    manifest_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "sources": [
                    {
                        "source_name": "Allowed manifest source",
                        "source_url": "https://example.test/allowed",
                        "normalized_output_allowed": True,
                        "license_notes": "Official public source; normalized metadata only.",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    result = subprocess.run(
        [
            sys.executable,
            "scripts/audit_source_licenses.py",
            "--edge-tsv-path",
            str(edge_path),
            "--source-manifest-path",
            str(manifest_path),
            "--as-of-date",
            "2026-07-01",
        ],
        check=True,
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
    )

    payload = json.loads(result.stdout)
    assert payload["status"] == "clean"
    assert payload["audit_metadata"]["as_of_date"] == "2026-07-01"
    assert payload["license_warning_counts"] == {}
    assert payload["summary"]["allowed_source_count"] == 2


def test_quarterly_source_license_audit_defers_legacy_tsv_to_manifest(
    tmp_path: Path,
) -> None:
    edge_path = tmp_path / "legacy_edges.tsv"
    manifest_path = tmp_path / "source_manifest.json"
    with edge_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            delimiter="\t",
            fieldnames=["source_ref", "target_ref", "relation", "source_url"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "source_ref": "ades:org:legacy",
                "target_ref": "ades:issuer:legacy",
                "relation": "org_part_of_holding",
                "source_url": "https://example.test/legacy",
            }
        )
    manifest_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "sources": [
                    {
                        "source_name": "Legacy lane manifest source",
                        "source_url": "https://example.test/legacy",
                        "normalized_output_allowed": True,
                        "license_notes": "Official public source; normalized metadata only.",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    payload = _audit_source_licenses(
        edge_tsv_paths=[edge_path],
        source_manifest_paths=[manifest_path],
        as_of_date=date(2026, 7, 1),
    )

    assert payload["status"] == "clean"
    assert payload["summary"]["source_row_count"] == 1
    assert payload["summary"]["legacy_source_row_count"] == 1
    assert payload["summary"]["allowed_source_count"] == 1
    assert payload["license_warning_counts"] == {}


def test_quarterly_source_license_audit_flags_legacy_tsv_without_manifest(
    tmp_path: Path,
) -> None:
    edge_path = tmp_path / "legacy_edges.tsv"
    with edge_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            delimiter="\t",
            fieldnames=["source_ref", "target_ref", "relation", "source_url"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "source_ref": "ades:org:legacy",
                "target_ref": "ades:issuer:legacy",
                "relation": "org_part_of_holding",
                "source_url": "https://example.test/legacy",
            }
        )

    payload = _audit_source_licenses(
        edge_tsv_paths=[edge_path],
        as_of_date=date(2026, 7, 1),
    )

    assert payload["status"] == "needs_license_review"
    assert payload["summary"]["source_row_count"] == 1
    assert payload["summary"]["legacy_source_row_count"] == 1
    assert payload["summary"]["blocked_source_count"] == 1
    assert payload["license_warning_counts"] == {"missing_license_metadata": 1}
    assert payload["action_items"] == [
        {
            "owner": "Ops/ADES",
            "category": "source_license",
            "warning": "missing_license_metadata",
            "count": 1,
            "action": "supply_source_manifest_or_add_license_metadata_before_promotion",
        }
    ]
