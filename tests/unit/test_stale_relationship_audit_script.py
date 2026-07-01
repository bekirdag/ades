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
        "effective_start_date": "2024-01-01",
        "effective_end_date": "",
        "confidence": "0.9",
        "notes": "",
        "fetched_at": "2026-05-01T00:00:00Z",
        "content_hash": "sha256:abc123",
        "review_status": "accepted",
        "license_status": "allowed",
        "license_notes": "public source",
        "confidence_basis": "official source page",
        "refresh_policy": "monthly",
    }
    row.update(overrides)
    return row


def _audit_stale_relationships(**kwargs) -> dict[str, object]:
    if str(SRC_ROOT) not in sys.path:
        sys.path.insert(0, str(SRC_ROOT))
    from scripts.audit_stale_relationships import audit_stale_relationships

    return audit_stale_relationships(**kwargs)


def test_monthly_stale_relationship_audit_flags_relationship_and_source_rows(
    tmp_path: Path,
) -> None:
    edge_path = tmp_path / "canonical_edges.tsv"
    _write_canonical_edges(
        edge_path,
        [
            _canonical_edge(
                {
                    "source_ref": "ades:org:operating-alpha",
                    "source_type": "organization",
                    "target_ref": "ades:holding:alpha",
                    "target_type": "holding_company",
                    "relation": "org_part_of_holding",
                    "effective_start_date": "2000-01-01",
                    "source_snapshot": "2026-05-01",
                    "fetched_at": "2026-05-01T00:00:00Z",
                }
            ),
            _canonical_edge(
                {
                    "source_ref": "ades:issuer:alpha",
                    "source_type": "issuer",
                    "target_ref": "ades:security:alpha-common",
                    "target_type": "security",
                    "relation": "issuer_has_security",
                    "effective_start_date": "2026-01-01",
                    "fetched_at": "2026-06-20T00:00:00Z",
                }
            ),
            _canonical_edge(
                {
                    "source_ref": "ades:issuer:beta",
                    "source_type": "issuer",
                    "target_ref": "ades:security:beta-common",
                    "target_type": "security",
                    "relation": "issuer_has_security",
                    "effective_start_date": "2026-01-01",
                    "source_snapshot": "",
                    "source_year": "",
                    "fetched_at": "",
                    "refresh_policy": "monthly",
                }
            ),
            _canonical_edge(
                {
                    "source_ref": "ades:issuer:gamma",
                    "source_type": "issuer",
                    "target_ref": "ades:security:gamma-common",
                    "target_type": "security",
                    "relation": "issuer_has_security",
                    "effective_start_date": "2026-01-01",
                    "source_snapshot": (
                        "/mnt/githubActions/ades_big_data/source_lane_test/"
                        "2026-06-29/raw_sources/gamma.html"
                    ),
                    "fetched_at": "",
                    "refresh_policy": "monthly",
                }
            ),
        ],
    )

    payload = _audit_stale_relationships(
        edge_tsv_paths=[edge_path],
        as_of_date=date(2026, 7, 1),
        sample_limit=10,
    )

    assert payload["status"] == "needs_refresh"
    assert payload["stale_relationship_warning_counts"] == {
        "suspiciously_old_ownership_relationship": 1,
    }
    assert payload["source_refresh_warning_counts"] == {
        "missing_source_refresh_date:monthly": 1,
        "source_refresh_due:monthly": 1,
    }
    source_samples = {
        sample["warning"]: sample for sample in payload["source_refresh_samples"]
    }
    assert source_samples["source_refresh_due:monthly"]["source_ref"] == (
        "ades:org:operating-alpha"
    )
    assert source_samples["source_refresh_due:monthly"]["source_date"] == "2026-05-01"
    assert source_samples["missing_source_refresh_date:monthly"]["source_ref"] == (
        "ades:issuer:beta"
    )
    assert "ades:issuer:gamma" not in {
        sample["source_ref"] for sample in payload["source_refresh_samples"]
    }
    assert {item["category"] for item in payload["action_items"]} == {
        "ownership_relationship",
        "source_refresh",
    }


def test_monthly_stale_relationship_audit_cli_emits_stable_json(tmp_path: Path) -> None:
    edge_path = tmp_path / "canonical_edges.tsv"
    _write_canonical_edges(
        edge_path,
        [
            _canonical_edge(
                {
                    "source_ref": "ades:issuer:alpha",
                    "source_type": "issuer",
                    "target_ref": "ades:security:alpha-common",
                    "target_type": "security",
                    "relation": "issuer_has_security",
                    "effective_start_date": "2026-01-01",
                    "source_snapshot": "2026-06",
                    "fetched_at": "",
                }
            ),
        ],
    )

    result = subprocess.run(
        [
            sys.executable,
            "scripts/audit_stale_relationships.py",
            "--edge-tsv-path",
            str(edge_path),
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
    assert payload["source_refresh_warning_counts"] == {}
    assert payload["stale_relationship_warning_counts"] == {}
